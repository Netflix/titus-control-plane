/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.netflix.titus.master.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.spectator.api.Registry;
import io.netflix.titus.api.audit.model.AuditLogEvent;
import io.netflix.titus.api.audit.service.AuditLogService;
import io.netflix.titus.api.model.event.TaskStateChangeEvent;
import io.netflix.titus.api.model.v2.JobCompletedReason;
import io.netflix.titus.api.model.v2.ServiceJobProcesses;
import io.netflix.titus.api.model.v2.V2JobDefinition;
import io.netflix.titus.api.model.v2.V2JobState;
import io.netflix.titus.api.model.v2.WorkerNaming;
import io.netflix.titus.api.model.v2.descriptor.SchedulingInfo;
import io.netflix.titus.api.model.v2.descriptor.StageSchedulingInfo;
import io.netflix.titus.api.store.v2.InvalidJobException;
import io.netflix.titus.api.store.v2.V2JobMetadata;
import io.netflix.titus.api.store.v2.V2StageMetadata;
import io.netflix.titus.api.store.v2.V2WorkerMetadata;
import io.netflix.titus.common.util.rx.eventbus.RxEventBus;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.MetricConstants;
import io.netflix.titus.master.config.MasterConfiguration;
import io.netflix.titus.master.job.V2JobOperations;
import io.netflix.titus.master.job.worker.WorkerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscriber;
import rx.functions.Func2;


@Singleton
public final class V2JobStore {

    private static class TerminatedJob implements Comparable<TerminatedJob> {
        private final String jobId;
        private final long terminatedTime;

        private TerminatedJob(String jobId, long terminatedTime) {
            this.jobId = jobId;
            this.terminatedTime = terminatedTime;
        }

        @Override
        public int compareTo(TerminatedJob o) {
            return Long.compare(terminatedTime, o.terminatedTime);
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(V2JobStore.class);
    private final ScheduledThreadPoolExecutor executor;
    private final AuditLogService auditLogService;
    private final Func2<V2JobStore, Map<String, V2JobDefinition>, Collection<NamedJob>> jobsInitializer;
    private final V2StorageProvider storageProvider;
    private final ConcurrentMap<String, V2JobMetadataWritable> activeJobsMap;
    private final ConcurrentMap<String, String> archivedJobIds;
    private static final long DELETE_TERMINATED_JOBS_INITIAL_DELAY_SECS = 120;
    private static final long DELETE_TERMINATED_JOBS_DELAY_SECS = 60;
    private final PriorityBlockingQueue<TerminatedJob> terminatedJobsToDelete;
    private final V2JobOperations jobOps;
    private final RxEventBus eventBus;
    private final MasterConfiguration config;
    private static final String initMillisGaugeName = "JobStoreInitMillis";
    private final AtomicLong initMillis;
    private static final String postInitMillisGaugeName = "JobStorePostInitMillis";
    private final AtomicLong postInitMillis;
    private final AtomicBoolean archivedJobsReady = new AtomicBoolean();

    @Inject
    public V2JobStore(V2StorageProvider storageProvider,
                      AuditLogService auditLogService,
                      V2JobOperations jobOps, RxEventBus eventBus,
                      MasterConfiguration config,
                      Registry registry) {
        this.storageProvider = storageProvider;
        this.auditLogService = auditLogService;
        this.jobOps = jobOps;
        this.eventBus = eventBus;
        this.config = config;
        this.jobsInitializer = jobOps.getJobsInitializer();
        activeJobsMap = new ConcurrentHashMap<>();
        archivedJobIds = new ConcurrentHashMap<>();
        terminatedJobsToDelete = new PriorityBlockingQueue<>();

        initMillis = registry.gauge(MetricConstants.METRIC_STORE + initMillisGaugeName, new AtomicLong());
        postInitMillis = registry.gauge(MetricConstants.METRIC_STORE + postInitMillisGaugeName, new AtomicLong());

        this.executor = new ScheduledThreadPoolExecutor(1);
        configureAuditLogEventsSubscriber();
    }

    private void configureAuditLogEventsSubscriber() {
        auditLogService.auditLogEvents().subscribe(new Subscriber<AuditLogEvent>() {
            final int MAX_AUDIT_LOG = 100;

            @Override
            public void onCompleted() {
                logger.error("Job store observable completed");
            }

            @Override
            public void onError(Throwable e) {
                logger.error("Error in job store observable: " + e.getMessage(), e);
            }

            @Override
            public void onNext(AuditLogEvent event) {
                try {
                    switch (event.getType()) {
                        case JOB_SCALE_DOWN:
                        case JOB_SCALE_UP:
                        case JOB_SCALE_UPDATE:
                        case JOB_PROCESSES_UPDATE:
                        case JOB_TERMINATE:
                            final String jobId = event.getOperand();
                            final V2JobMetadataWritable job = activeJobsMap.get(jobId);
                            if (job != null) {
                                final List<AuditLogEvent> latestAuditLogEvents = job.getLatestAuditLogEvents();
                                latestAuditLogEvents.add(event);
                                while (latestAuditLogEvents.size() >= MAX_AUDIT_LOG) {
                                    latestAuditLogEvents.remove(0);
                                }
                                try (AutoCloseable ac = job.obtainLock()) {
                                    storageProvider.updateJob(job);
                                } catch (Exception e) {
                                    logger.error("Exception storing audit log event job " + job.getJobId(), e);
                                }
                            }
                            break;
                        default:
                            break;
                    }
                } catch (Exception ex) {
                    logger.error("Exception in AuditLogEvent subscriber (V2JobStore) ", ex);
                }
            }
        });
    }

    private void deleteOldTerminatedJobs() {
        final long tooOldCutOff = System.currentTimeMillis() - (getTerminatedJobToDeleteDelayHours() * 3600000L);

        logger.debug("Terminating all jobs finished before {}", tooOldCutOff);

        int limit = config.getTerminatedJobCleanUpBatchSize();
        for (int i = 0; i < limit; i++) {
            TerminatedJob jobToDelete = terminatedJobsToDelete.poll();
            if (jobToDelete == null) {
                logger.debug("Terminated jobs queue empty");
                return;
            }
            if (jobToDelete.terminatedTime > (tooOldCutOff)) {
                // job not ready to be deleted yet, add it back
                logger.debug("No jobs to be evicted in the job termination queue; the latest one is {} due at {}",
                        jobToDelete.jobId, jobToDelete.terminatedTime
                );
                terminatedJobsToDelete.add(jobToDelete);
                return;
            }
            logger.info("Deleting old job " + jobToDelete.jobId);
            try {
                if (jobOps.deleteJob(jobToDelete.jobId)) {
                    activeJobsMap.remove(jobToDelete.jobId);
                }
            } catch (Throwable e) {
                logger.error("Job {} delete failed", jobToDelete.jobId, e);
            }
        }
    }

    private long getTerminatedJobToDeleteDelayHours() {
        return config.getTerminatedJobToDeleteDelayHours();
    }

    public V2JobMetadata storeNewJob(String jobId, String jobName, V2JobDefinition jobDefinition)
            throws IOException, JobAlreadyExistsException {
        V2JobMetadataWritable jobMetadata = new V2JobMetadataWritable(jobId, jobName,
                jobDefinition.getUser(), System.currentTimeMillis(), jobDefinition.getJobJarFileLocation(),
                jobDefinition.getSchedulingInfo().getStages().size(),
                jobDefinition.getJobSla(), V2JobState.Accepted,
                jobDefinition.getSubscriptionTimeoutSecs(),
                jobDefinition.getParameters(), 1);
        storageProvider.storeNewJob(jobMetadata);
        activeJobsMap.put(jobMetadata.getJobId(), jobMetadata);
        return jobMetadata;
    }

    public void updateJob(String jobId, V2JobMetadata jobMetadata) throws InvalidJobException, IOException {
        if (jobMetadata == null) {
            throw new NullPointerException("Null job metadata");
        }
        if (jobId == null) {
            throw new NullPointerException("Null job Id");
        }
        if (!jobId.equals(jobMetadata.getJobId())) {
            throw new InvalidJobException(jobId, new Exception("Mismatched jobId=" + jobId + " with metadata.jobId=" +
                    jobMetadata.getJobId()));
        }
        if (activeJobsMap.replace(jobId, (V2JobMetadataWritable) jobMetadata) == null) {
            throw new InvalidJobException(jobId);
        }
        storageProvider.updateJob((V2JobMetadataWritable) jobMetadata);
    }

    public V2StorageProvider getStorageProvider() {
        return storageProvider;
    }

    public void storeJobState(String jobId, V2JobState state)
            throws InvalidJobException, IOException, InvalidJobStateChangeException {
        V2JobMetadataWritable job = activeJobsMap.get(jobId);
        if (job == null) {
            throw new InvalidJobException(jobId);
        }
        job.setJobState(state);
        storageProvider.updateJob(job);
        if (V2JobState.isTerminalState(state)) {
            terminatedJobsToDelete.add(new TerminatedJob(jobId, System.currentTimeMillis()));
            archiveJob(job);
            activeJobsMap.remove(jobId);
            archivedJobIds.put(jobId, jobId);
            jobOps.terminateJob(jobId);
        }
    }

    private void archiveJob(V2JobMetadataWritable job) throws IOException {
        storageProvider.archiveJob(job.getJobId());
    }

    public void storeJobNextWorkerNumber(String jobId, int n)
            throws InvalidJobException, IOException {
        V2JobMetadataWritable job = activeJobsMap.get(jobId);
        if (job == null) {
            throw new InvalidJobException(jobId);
        }
        job.setNextWorkerNumberToUse(n);
        storageProvider.updateJob(job);
    }

    public void deleteJob(String jobId) throws IOException, InvalidJobException {
        storageProvider.deleteJob(jobId);
        activeJobsMap.remove(jobId);
        archivedJobIds.remove(jobId);
        auditLogService.submit(new AuditLogEvent(AuditLogEvent.Type.JOB_DELETE, jobId, "", System.currentTimeMillis()));
    }

    public Collection<String> getTerminatedJobIds() {
        return new LinkedList<>(archivedJobIds.keySet());
    }

    public List<? extends V2WorkerMetadata> storeNewWorkers(List<WorkerRequest> workerRequests)
            throws IOException, InvalidJobException {
        if (workerRequests == null || workerRequests.isEmpty()) {
            return null;
        }
        String jobId = workerRequests.get(0).getJobId();
        logger.info("Adding " + workerRequests.size() + " workers for job " + jobId);
        V2JobMetadataWritable job = activeJobsMap.get(jobId);
        if (job == null) {
            throw new InvalidJobException(jobId, -1, -1);
        }
        List<V2WorkerMetadataWritable> addedWorkers = new ArrayList<>();
        for (WorkerRequest workerRequest : workerRequests) {
            if (job.getStageMetadata(workerRequest.getWorkerStage()) == null) {

                V2StageMetadataWritable msmd = new V2StageMetadataWritable(workerRequest.getJobId(),
                        workerRequest.getWorkerStage(), workerRequest.getTotalStages(), workerRequest.getDefinition(),
                        workerRequest.getNumInstancesAtStage(), workerRequest.getHardConstraints(),
                        workerRequest.getSoftConstraints(), workerRequest.getSecurityGroups(), workerRequest.getAllocateIP(),
                        workerRequest.getSchedulingInfo().forStage(workerRequest.getWorkerStage()).getScalingPolicy(),
                        workerRequest.getSchedulingInfo().forStage(workerRequest.getWorkerStage()).getScalable(),
                        ServiceJobProcesses.newBuilder().build());
                boolean added = job.addJobStageIfAbsent(msmd);
                if (added) {
                    storageProvider.storeStage(msmd); // store the new
                }
            }
            V2WorkerMetadataWritable mwmd = new V2WorkerMetadataWritable(workerRequest.getWorkerIndex(),
                    workerRequest.getWorkerNumber(), workerRequest.getJobId(), workerRequest.getWorkerInstanceId(),
                    workerRequest.getWorkerStage(), workerRequest.getNumPortsPerInstance());
            if (!job.addWorkerMedata(workerRequest.getWorkerStage(), mwmd, null)) {
                V2WorkerMetadata tmp = job.getWorkerByIndex(workerRequest.getWorkerStage(), workerRequest.getWorkerIndex());
                throw new InvalidJobException(job.getJobId(), workerRequest.getWorkerStage(), workerRequest.getWorkerIndex(),
                        new Exception("Couldn't add worker " + workerRequest.getWorkerNumber() + " as index " +
                                workerRequest.getWorkerIndex() + ", that index already has worker " +
                                tmp.getWorkerNumber()));
            }
            addedWorkers.add(mwmd);
        }
        storageProvider.storeWorkers(jobId, addedWorkers);
        addedWorkers.forEach(worker -> sendNewWorkerEvent(job, worker));
        return addedWorkers;
    }

    public V2WorkerMetadata storeNewWorker(WorkerRequest workerRequest)
            throws IOException, InvalidJobException {
        //logger.info("Adding worker index=" + workerRequest.getWorkerIndex());
        V2JobMetadataWritable job = activeJobsMap.get(workerRequest.getJobId());
        if (job == null) {
            throw new InvalidJobException(workerRequest.getJobId(), workerRequest.getWorkerStage(), workerRequest.getWorkerIndex());
        }
        if (job.getStageMetadata(workerRequest.getWorkerStage()) == null) {
            V2StageMetadataWritable msmd = new V2StageMetadataWritable(workerRequest.getJobId(),
                    workerRequest.getWorkerStage(), workerRequest.getTotalStages(), workerRequest.getDefinition(),
                    workerRequest.getNumInstancesAtStage(), workerRequest.getHardConstraints(),
                    workerRequest.getSoftConstraints(), workerRequest.getSecurityGroups(), workerRequest.getAllocateIP(),
                    workerRequest.getSchedulingInfo().forStage(workerRequest.getWorkerStage()).getScalingPolicy(),
                    workerRequest.getSchedulingInfo().forStage(workerRequest.getWorkerStage()).getScalable(),
                    ServiceJobProcesses.newBuilder().build());
            boolean added = job.addJobStageIfAbsent(msmd);
            if (added) {
                storageProvider.storeStage(msmd); // store the new
            }
        }
        V2WorkerMetadataWritable mwmd = new V2WorkerMetadataWritable(workerRequest.getWorkerIndex(),
                workerRequest.getWorkerNumber(), workerRequest.getJobId(), workerRequest.getWorkerInstanceId(),
                workerRequest.getWorkerStage(), workerRequest.getNumPortsPerInstance());
        if (!job.addWorkerMedata(workerRequest.getWorkerStage(), mwmd, null)) {
            V2WorkerMetadata tmp = job.getWorkerByIndex(workerRequest.getWorkerStage(), workerRequest.getWorkerIndex());
            throw new InvalidJobException(job.getJobId(), workerRequest.getWorkerStage(), workerRequest.getWorkerIndex(),
                    new Exception("Couldn't add worker " + workerRequest.getWorkerNumber() + " as index " +
                            workerRequest.getWorkerIndex() + ", that index already has worker " +
                            tmp.getWorkerNumber()));
        }
        storageProvider.storeWorker(mwmd);
        sendNewWorkerEvent(job, mwmd);
        return mwmd;
    }

    private void sendNewWorkerEvent(V2JobMetadata job, V2WorkerMetadata worker) {
        String taskId = WorkerNaming.getTaskId(worker);
        eventBus.publish(new TaskStateChangeEvent<>(worker.getJobId(), taskId, worker.getState(), System.currentTimeMillis(), Pair.of(job, worker)));
    }

    public boolean storeStage(V2StageMetadata msmd) throws IOException, InvalidJobException {
        final V2JobMetadataWritable job = activeJobsMap.get(msmd.getJobId());
        if (job == null) {
            throw new InvalidJobException(msmd.getJobId(), msmd.getStageNum(), -1);
        }
        if (job.addJobStageIfAbsent((V2StageMetadataWritable) msmd)) {
            storageProvider.storeStage((V2StageMetadataWritable) msmd);
            return true;
        }
        return false;
    }

    public void updateStage(V2StageMetadata msmd)
            throws IOException, InvalidJobException {
        V2JobMetadataWritable job = activeJobsMap.get(msmd.getJobId());
        if (job == null) {
            throw new InvalidJobException(msmd.getJobId(), msmd.getStageNum(), -1);
        }
        storageProvider.updateStage((V2StageMetadataWritable) msmd);
    }

    /**
     * Atomically replace worker with new one created from the given worker request.
     *
     * @param workerRequest
     * @param replacedWorker
     * @return The newly created worker.
     * @throws IOException                    Upon error from storage provider.
     * @throws InvalidJobException            If there is no such job or stage referred to in the worker metadata.
     * @throws InvalidJobStateChangeException If the replaced worker's state cannot be changed. In which case no new
     *                                        worker is created.
     */
    public V2WorkerMetadata replaceTerminatedWorker(WorkerRequest workerRequest, V2WorkerMetadata replacedWorker)
            throws IOException, InvalidJobException, InvalidJobStateChangeException {
        logger.info("Replacing worker index=" + workerRequest.getWorkerIndex() + " number=" + replacedWorker.getWorkerNumber() +
                " with number=" + workerRequest.getWorkerNumber());
        V2JobMetadataWritable job = activeJobsMap.get(workerRequest.getJobId());
        if (job == null) {
            throw new InvalidJobException(workerRequest.getJobId(), workerRequest.getWorkerStage(), workerRequest.getWorkerIndex());
        }
        if (job.getStageMetadata(workerRequest.getWorkerStage()) == null) {
            throw new InvalidJobException(workerRequest.getJobId(), workerRequest.getWorkerStage(), replacedWorker.getWorkerIndex());
        }
        if (!V2JobState.isTerminalState(replacedWorker.getState())) {
            throw new InvalidJobStateChangeException(replacedWorker.getJobId(), replacedWorker.getState());
        }
        V2WorkerMetadataWritable mwmd = new V2WorkerMetadataWritable(workerRequest.getWorkerIndex(),
                workerRequest.getWorkerNumber(), workerRequest.getJobId(), workerRequest.getWorkerInstanceId(),
                workerRequest.getWorkerStage(), workerRequest.getNumPortsPerInstance());
        mwmd.setResubmitInfo(replacedWorker.getWorkerNumber(), replacedWorker.getTotalResubmitCount() + 1);
        if (!job.addWorkerMedata(replacedWorker.getStageNum(), mwmd, replacedWorker)) {
            throw new InvalidJobStateChangeException(replacedWorker.getJobId(), replacedWorker.getState(), V2JobState.Failed);
        }
        storageProvider.storeAndUpdateWorkers(mwmd, (V2WorkerMetadataWritable) replacedWorker);

        V2StageMetadataWritable msmd = (V2StageMetadataWritable) job.getStageMetadata(replacedWorker.getStageNum());
        if (msmd.removeWorkerInTerminalState(replacedWorker.getWorkerNumber()) != null) {
            archiveWorker((V2WorkerMetadataWritable) replacedWorker);
        }
        return mwmd;
    }

    public void storeWorkerState(String jobId, int workerNumber, V2JobState state, JobCompletedReason reason)
            throws InvalidJobException, InvalidJobStateChangeException, IOException {
        storeWorkerState(jobId, workerNumber, state, reason, true);
    }

    public void storeWorkerState(String jobId, int workerNumber, V2JobState state, JobCompletedReason reason,
                                 boolean archiveIfError)
            throws InvalidJobException, InvalidJobStateChangeException, IOException {
        V2WorkerMetadataWritable mwmd = (V2WorkerMetadataWritable) getWorkerByNumber(jobId, workerNumber);
        mwmd.setState(state, System.currentTimeMillis(), reason);
        if (logger.isDebugEnabled()) {
            String taskId = WorkerNaming.getWorkerName(jobId, mwmd.getWorkerIndex(), mwmd.getWorkerNumber());
            logger.debug("Persisting worker {} state={}, reason={}", taskId, state, reason);
        }
        storageProvider.updateWorker(mwmd);
        if (archiveIfError && V2JobState.isErrorState(state)) {
            final V2JobMetadata activeJob = getActiveJob(jobId);
            if (activeJob != null) {
                V2StageMetadataWritable msmd = (V2StageMetadataWritable) activeJob.getStageMetadata(mwmd.getStageNum());
                if (msmd.removeWorkerInTerminalState(workerNumber) != null) {
                    archiveWorker(mwmd);
                }
            }
        }
    }

    public void storeWorkerMigrationDeadline(String jobId, int workerNumber, long migrationDeadline)
            throws InvalidJobException, InvalidJobStateChangeException, IOException {
        V2WorkerMetadataWritable mwmd = (V2WorkerMetadataWritable) getWorkerByNumber(jobId, workerNumber);
        mwmd.setMigrationDeadline(migrationDeadline);
        storageProvider.updateWorker(mwmd);
    }

    public void archiveWorker(V2WorkerMetadataWritable mwmd) throws IOException {
        storageProvider.archiveWorker(mwmd);
    }

    public List<? extends V2WorkerMetadata> getArchivedWorkers(String jobId) throws IOException {
        List<V2WorkerMetadataWritable> workers = storageProvider.getArchivedWorkers(jobId);
        if (workers == null) {
            return Collections.emptyList();
        }
        return workers;
    }

    public V2WorkerMetadata getArchivedWorker(String jobId, int workerNumber) throws IOException {
        List<? extends V2WorkerMetadata> archivedWorkers = getArchivedWorkers(jobId);
        for (V2WorkerMetadata w : archivedWorkers) {
            if (w.getWorkerNumber() == workerNumber) {
                return w;
            }
        }
        return null;
    }

    private V2WorkerMetadata getWorkerByNumber(String jobId, int workerNumber) throws InvalidJobException {
        V2JobMetadata mjmd = activeJobsMap.get(jobId);
        if (mjmd == null) {
            throw new InvalidJobException(jobId);
        }
        return mjmd.getWorkerByNumber(workerNumber);
    }

    /**
     * Get the job metadata object for the given Id.
     *
     * @param jobId Job ID.
     * @return Job object if it exists, null otherwise.
     */
    public V2JobMetadata getActiveJob(String jobId) {
        return activeJobsMap.get(jobId);
    }

    public V2JobMetadata getCompletedJob(String jobId) throws IOException {
        return storageProvider.loadArchivedJob(jobId);
    }

    static SchedulingInfo getSchedulingInfo(V2JobMetadata mjmd) {
        int numStages = mjmd.getNumStages();
        logger.info(mjmd.getJobId() + ": numStages=" + numStages);
        Map<Integer, StageSchedulingInfo> stagesMap = new HashMap<>();

        // Note that job maintains stage numbers starting with 1, not 0.
        for (int s = 1; s <= numStages; s++) {
            V2StageMetadata stageMetadata = mjmd.getStageMetadata(s);

            stagesMap.put(s, new StageSchedulingInfo(stageMetadata.getNumWorkers(), stageMetadata.getMachineDefinition(),
                    stageMetadata.getHardConstraints(), stageMetadata.getSoftConstraints(),
                    stageMetadata.getSecurityGroups(), stageMetadata.getAllocateIP(), stageMetadata.getScalingPolicy(),
                    stageMetadata.getScalable()));
        }
        return new SchedulingInfo(stagesMap);
    }

    final public void start() {
        logger.info("Titus store starting now");
        long st = System.currentTimeMillis();
        final List<V2JobMetadataWritable> jobsToArchive = new LinkedList<>();
        final List<V2JobMetadataWritable> invalidJobs = new ArrayList<>();
        final AtomicReference<Collection<NamedJob>> ref = new AtomicReference<>();
        try {
            for (V2JobMetadataWritable mjmd : storageProvider.initJobs()) {
                try {
                    validateJob(mjmd);
                } catch (InvalidJobException e) {
                    invalidJobs.add(mjmd);
                    logger.warn("Skipping job due to invalid details: ", e);
                    continue;
                }
                if (V2JobState.isTerminalState(mjmd.getState())) {
                    jobsToArchive.add(mjmd);
                } else {
                    activeJobsMap.put(mjmd.getJobId(), mjmd);
                }
            }
            List<String> ids = new ArrayList<>();
            for (V2JobMetadataWritable invalidJob : invalidJobs) {
                logger.info("JobId: {} failed to initialize: {}", invalidJob.getJobId(), invalidJob);
                ids.add(invalidJob.getJobId());
            }
            if (!ids.isEmpty()) {
                logger.info("Failed to load {} jobs with ids: {}", ids.size(), ids);
            }
            if (invalidJobs.size() > config.getMaxInvalidJobs()) {
                logger.error("Exiting because the number of invalid jobs was greater than: {}", config.getMaxInvalidJobs());
                System.exit(1);
            }

            logger.info("Read " + activeJobsMap.size() + " job records from persistence in " + (System.currentTimeMillis() - st) + " ms");
            if (jobsInitializer != null) {
                Map<String, V2JobDefinition> jobDefsMap = new HashMap<>();
                for (V2JobMetadata mjmd : activeJobsMap.values()) {
                    jobDefsMap.put(mjmd.getJobId(), new V2JobDefinition(mjmd.getName(), mjmd.getUser(), mjmd.getJarUrl(), "",
                            mjmd.getParameters(), mjmd.getSla(), mjmd.getSubscriptionTimeoutSecs(), getSchedulingInfo(mjmd),
                            0, 0, null, null)); // min/max don't matter for job instance
                }
                ref.set(jobsInitializer.call(V2JobStore.this, jobDefsMap));
            }
        } catch (Throwable e /* catch a Throwable only because we are going to System.exit right away */) {
            logger.error(String.format("Exiting due to storage init failure: %s: ", e.getMessage()), e);
            System.exit(1); // can't deal with storage error
        }
        initMillis.set(System.currentTimeMillis() - st);
        logger.info("Enabling terminated jobs removal from the storage");
        executor.scheduleWithFixedDelay(this::deleteOldTerminatedJobs,
                DELETE_TERMINATED_JOBS_INITIAL_DELAY_SECS, DELETE_TERMINATED_JOBS_DELAY_SECS, TimeUnit.SECONDS
        );
        new Thread(() -> {
            try {
                doPostInit(jobsToArchive, ref.get());
            } catch (Throwable e) {
                logger.error("Archived state initialization failure", e);
            }
        }).start();
    }

    private long getTerminatedAt(V2JobMetadata mjmd) {
        long terminatedAt = mjmd.getSubmittedAt();
        for (V2StageMetadata msmd : mjmd.getStageMetadata()) {
            for (V2WorkerMetadata mwmd : msmd.getAllWorkers()) {
                terminatedAt = Math.max(terminatedAt, mwmd.getCompletedAt());
            }
        }
        return terminatedAt;
    }

    private void doPostInit(List<V2JobMetadataWritable> jobsToArchive, Collection<NamedJob> namedJobs) {
        long start1 = System.currentTimeMillis();
        final AtomicInteger count = new AtomicInteger();
        storageProvider.initArchivedJobs()
                .doOnNext(job -> {
                    archivedJobIds.put(job.getJobId(), job.getJobId());
                    terminatedJobsToDelete.add(new TerminatedJob(job.getJobId(), getTerminatedAt(job)));
                    count.incrementAndGet();
                    if (count.get() % 1000 == 0) {
                        logger.info("Archived data loading in progress. Already loaded {}...", count.get());
                    }
                })
                .toBlocking()
                .lastOrDefault(null);
        logger.info("Read " + count.get() + " archived job records from persistence in " + (System.currentTimeMillis() - start1) + " ms");
        if (!jobsToArchive.isEmpty()) {
            logger.info("Starting finished jobs migration ({})...", jobsToArchive.size());
            long start2 = System.currentTimeMillis();
            Map<String, NamedJob> namedJobMap = new HashMap<>();
            if (!namedJobs.isEmpty()) {
                for (NamedJob nj : namedJobs) {
                    namedJobMap.put(nj.getName(), nj);
                }
            }
            int left = jobsToArchive.size();
            for (V2JobMetadataWritable job : jobsToArchive) {
                try {
                    archiveJob(job);
                    archivedJobIds.put(job.getJobId(), job.getJobId());
                    terminatedJobsToDelete.add(new TerminatedJob(job.getJobId(), getTerminatedAt(job)));
                    if (config.getStoreCompletedJobsForNamedJob()) {
                        final NamedJob namedJob = namedJobMap.get(job.getName());
                        if (namedJob != null) {
                            final NamedJob.CompletedJob completedJob = new NamedJob.CompletedJob(job.getName(), job.getJobId(),
                                    null, job.getState(), job.getSubmittedAt(), getTerminatedAt(job));
                            namedJob.initCompletedJob(completedJob);
                            storageProvider.storeCompletedJobForNamedJob(namedJob.getName(), completedJob);
                        }
                    }
                } catch (IOException e) {
                    logger.error("Error archiving job " + job.getJobId() + ": " + e.getMessage(), e);
                }
                left--;
                if (left % 1000 == 0) {
                    logger.info("Still has {} finished jobs to archive...", left);
                }
            }
            logger.info("Moved " + jobsToArchive.size() + " completed jobs to archived storage in " + (System.currentTimeMillis() - start2) + " ms");
        }
        postInitMillis.set(System.currentTimeMillis() - start1);
        archivedJobsReady.set(true);
    }

    private void validateJob(V2JobMetadataWritable job) throws InvalidJobException {
        if (job == null) {
            throw new InvalidJobException("Null job record");
        }
        if (job.getJobId() == null) {
            throw new InvalidJobException("Null jobId");
        }
        if (job.getStageMetadata(1) == null) {
            throw new InvalidJobException(job.getJobId() + ": No stage metadata found");
        }
    }

    public void shutdown() {
        activeJobsMap.clear();
        storageProvider.shutdown();
        executor.shutdown();
    }
}
