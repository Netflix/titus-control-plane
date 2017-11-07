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

package io.netflix.titus.master.job.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Ordering;
import com.google.inject.Injector;
import com.netflix.spectator.api.Registry;
import io.netflix.titus.api.audit.model.AuditLogEvent;
import io.netflix.titus.api.audit.service.AuditLogService;
import io.netflix.titus.api.jobmanager.model.job.ServiceJobProcesses;
import io.netflix.titus.api.jobmanager.model.job.sanitizer.JobConfiguration;
import io.netflix.titus.api.jobmanager.service.JobManagerException;
import io.netflix.titus.api.model.event.JobStateChangeEvent;
import io.netflix.titus.api.model.event.JobStateChangeEvent.JobState;
import io.netflix.titus.api.model.v2.JobCompletedReason;
import io.netflix.titus.api.model.v2.V2JobDefinition;
import io.netflix.titus.api.model.v2.V2JobState;
import io.netflix.titus.api.model.v2.WorkerNaming;
import io.netflix.titus.api.model.v2.descriptor.StageScalingPolicy;
import io.netflix.titus.api.model.v2.descriptor.StageSchedulingInfo;
import io.netflix.titus.api.model.v2.parameter.Parameters;
import io.netflix.titus.api.store.v2.InvalidJobException;
import io.netflix.titus.api.store.v2.V2JobMetadata;
import io.netflix.titus.api.store.v2.V2StageMetadata;
import io.netflix.titus.api.store.v2.V2WorkerMetadata;
import io.netflix.titus.common.util.rx.eventbus.RxEventBus;
import io.netflix.titus.master.JobSchedulingInfo;
import io.netflix.titus.master.MetricConstants;
import io.netflix.titus.master.config.MasterConfiguration;
import io.netflix.titus.master.job.BaseJobMgr;
import io.netflix.titus.master.job.JobManagerConfiguration;
import io.netflix.titus.master.job.JobMgrUtils;
import io.netflix.titus.master.job.JobUpdateException;
import io.netflix.titus.master.job.V2JobMgrIntf;
import io.netflix.titus.master.job.worker.WorkerRequest;
import io.netflix.titus.master.scheduler.SchedulingService;
import io.netflix.titus.master.store.InvalidJobStateChangeException;
import io.netflix.titus.master.store.InvalidNamedJobException;
import io.netflix.titus.master.store.NamedJob;
import io.netflix.titus.master.store.NamedJobs;
import io.netflix.titus.master.store.V2JobMetadataWritable;
import io.netflix.titus.master.store.V2JobStore;
import io.netflix.titus.master.store.V2StageMetadataWritable;
import io.netflix.titus.master.store.V2WorkerMetadataWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.subjects.ReplaySubject;

public class ServiceJobMgr extends BaseJobMgr {

    private static final Logger logger = LoggerFactory.getLogger(ServiceJobMgr.class);

    private static final String ROOT_METRICS_NAME = MetricConstants.METRIC_SCHEDULING_JOB + "service.";

    private final AuditLogService auditLogService;

    public ServiceJobMgr(Injector injector,
                         String jobId,
                         V2JobDefinition jobDefinition,
                         NamedJob namedJob,
                         Observer<Observable<JobSchedulingInfo>> jobSchedulingObserver,
                         RxEventBus eventBus,
                         Registry registry) {
        super(injector, jobId, true, jobDefinition, namedJob, jobSchedulingObserver,
                injector.getInstance(MasterConfiguration.class), injector.getInstance(JobManagerConfiguration.class),
                injector.getInstance(JobConfiguration.class),
                logger, ROOT_METRICS_NAME, eventBus, injector.getInstance(SchedulingService.class), registry);
        this.auditLogService = injector.getInstance(AuditLogService.class);
    }

    public static V2JobMgrIntf acceptSubmit(String user, V2JobDefinition jobDefinition, NamedJobs namedJobs,
                                            Injector injector,
                                            ReplaySubject<Observable<JobSchedulingInfo>> jobSchedulingObserver,
                                            RxEventBus eventBus, Registry registry, V2JobStore store) {
        Parameters.JobType jobType = Parameters.getJobType(jobDefinition.getParameters());
        if (jobType != Parameters.JobType.Service) {
            throw new IllegalArgumentException("Can't accept " + jobType + " job definition");
        }
        try {
            String jobId = createNewJobid(jobDefinition, namedJobs);
            V2JobMgrIntf jobMgr = new ServiceJobMgr(injector, jobId, jobDefinition,
                    namedJobs.getJobByName(jobDefinition.getName()), jobSchedulingObserver, eventBus, registry);
            jobMgr.initializeNewJob(store);
            return jobMgr;
        } catch (InvalidNamedJobException | InvalidJobException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    private static String createNewJobid(V2JobDefinition jobDefinition, NamedJobs namedJobs) throws InvalidNamedJobException {
        // TODO make this work with the namedJob created for this job definiiton
        final NamedJobs.JobIdForSubmit jobIdForSubmit = namedJobs.getJobIdForSubmit(jobDefinition.getName(), jobDefinition);
        return jobIdForSubmit.getJobId();
    }

    @Override
    protected boolean shouldResubmit(V2WorkerMetadata task, V2JobState newState) {
        if (V2JobState.isTerminalState(newState)) {
            resubmitRateLimiter.delayWorkerResubmit(jobId, task.getStageNum(), task.getWorkerIndex());
        }
        return true;
    }

    @Override
    public void initialize(V2JobStore store) {
        super.initialize(store);
        if (V2JobState.isTerminalState(getJobMetadata().getState())) {
            return;
        }

        // check to make sure all required tasks are running
        Set<Integer> tombStoneIndex = new HashSet<>(JobMgrUtils.getTombStoneSlots(getJobMetadata()));
        final V2StageMetadataWritable stageMetadata = (V2StageMetadataWritable) getJobMetadata().getStageMetadata(1);
        int allWorkers = tombStoneIndex.size() + stageMetadata.getNumWorkers();
        for (int i = 0; i < allWorkers; i++) {
            if (!tombStoneIndex.contains(i)) {
                try {
                    stageMetadata.getWorkerByIndex(i);
                } catch (InvalidJobException e) {
                    // worker for this index doesn't exist, go ahead create one
                    final List<WorkerRequest> workersInRange = getWorkersInRange(
                            i, i + 1, stageMetadata.getNumWorkers()
                    );
                    try {
                        List<? extends V2WorkerMetadata> newWorkers = store.storeNewWorkers(workersInRange);
                        newWorkers.forEach(w -> {
                            jobMetrics.updateTaskMetrics(w);
                            queueTask(w);
                        });
                    } catch (IOException e1) {
                        logger.error(jobId + ": Unexpected to fail storing new worker"); // ToDo how do we handle storage errors?
                    } catch (InvalidJobException e1) {
                        logger.error(jobId + ": Unexpected - " + e.getMessage()); // shouldn't happen, we are a valid job
                    }
                }
            }
        }

        Collection<V2WorkerMetadata> taskByIndexMetadataSet = Ordering.from(Integer::compare)
                .onResultOf(V2WorkerMetadata::getWorkerIndex)
                .sortedCopy(stageMetadata.getWorkerByIndexMetadataSet());

        boolean stageUpdated = false;

        // Enforce desired == numWorkers
        final StageScalingPolicy scalingPolicy = stageMetadata.getScalingPolicy();
        if (scalingPolicy != null && stageMetadata.getNumWorkers() != scalingPolicy.getDesired()) {
            stageMetadata.unsafeSetNumWorkers(scalingPolicy.getDesired());
            stageUpdated = true;
        }

        // Enforce active worker count <= numWorkers, terminating excessive workers. Reschedule not-running workers if needed.
        int activeCount = 0;
        for (V2WorkerMetadata t : taskByIndexMetadataSet) {
            boolean isTombStone = t.getState() == V2JobState.Failed && t.getReason() == JobCompletedReason.TombStone;
            if (!isTombStone) {
                if (activeCount == stageMetadata.getNumWorkers()) {
                    // This may happen when scaling policy was successfully updated, but workers were not updated in a storage.
                    try {
                        logger.warn("Removing excessive task {}-{}-{}", t.getJobId(), t.getWorkerIndex(), t.getWorkerNumber());
                        store.archiveWorker((V2WorkerMetadataWritable) t);
                        stageMetadata.unsafeRemoveWorker(t.getWorkerIndex(), t.getWorkerNumber());
                        stageUpdated = true;
                    } catch (IOException e) {
                        logger.warn("Cannot remove {}-{}-{}", t.getJobId(), t.getWorkerIndex(), t.getWorkerNumber(), e);
                    }
                } else {
                    if (V2JobState.isTerminalState(t.getState())) {
                        // replace this task
                        // ToDo need to do this periodically after init too?
                        try {
                            replaceWorker(t);
                        } catch (IOException | InvalidJobException | InvalidJobStateChangeException e) {
                            logger.error(jobId + ": Error storing new worker for index " + t.getWorkerIndex() + " - " + e.getMessage());
                        }
                    }
                    activeCount++;
                }
            }
        }

        // Fix numWorkers if actual number of workers hold is smaller
        if (activeCount < stageMetadata.getNumWorkers()) {
            logger.warn("Active task set < numWorkers ({} < {}); setting numWorkers={}", activeCount, stageMetadata.getNumWorkers(), stageMetadata.getNumWorkers());
            stageMetadata.unsafeSetNumWorkers(activeCount);
            stageUpdated = true;
        }
        if (stageUpdated) {
            try {
                logger.info("Job's {} stage metadata changed", jobId);
                store.updateStage(stageMetadata);
            } catch (Exception e) {
                logger.warn("Cannot update stage for job", jobId, e);
            }
        }

        // Align desired number of tasks, with the actual number that we currently have
        if (scalingPolicy == null) {
            logger.warn("Scaling policy not defined for task {}", jobId);
        } else if (scalingPolicy.getDesired() != stageMetadata.getNumWorkers()) {
            logger.warn(jobId + ": overwriting scalingPolicy.desired=" + scalingPolicy.getDesired() +
                    " onto current workers count of " + stageMetadata.getNumWorkers());
            try {
                updateInstanceCounts(scalingPolicy.getMin(), scalingPolicy.getDesired(), scalingPolicy.getMax(),
                        "Master", stageMetadata.getNumWorkers());
            } catch (InvalidJobException e) {
                logger.warn("Unexpected error setting instance counts per scaling policy: " + e.getMessage());
            }
        }
    }

    @Override
    public boolean killTask(String taskId, String user, String reason) {
        final WorkerNaming.JobWorkerIdPair jobAndWorkerId = WorkerNaming.getJobAndWorkerId(taskId);
        final V2WorkerMetadata task = getTask(jobAndWorkerId.workerNumber, false);
        if (task == null ||
                V2JobState.isTerminalState(task.getState()) ||
                V2JobState.isTerminalState(getJobMetadata().getState())
                ) {
            return super.killTask(taskId, user, reason);
        } else {
            try {
                resubmitWorker(taskId, "kill requested by: " + user + ", resubmitting");
                return true;
            } catch (InvalidJobException | InvalidJobStateChangeException e) {
                logger.warn(jobId + ": couldn't kill task " + taskId + " invoked by user " + user + ": "
                        + e.getMessage());
                return false;
            }
        }
    }

    @Override
    public boolean killTaskAndShrink(String taskId, String user) {
        V2JobMetadata jobMetadata = getJobMetadata();
        V2StageMetadata stageMetadata = jobMetadata.getStageMetadata(1);
        StageScalingPolicy scalingPolicy = stageMetadata.getScalingPolicy();

        ServiceJobProcesses jobProcesses = stageMetadata.getJobProcesses();
        if (jobProcesses != null) {
            int targetDesired = scalingPolicy.getDesired() - 1;
            if (isTargetDesiredCountInvalid(targetDesired, scalingPolicy, jobProcesses)) {
                throw JobManagerException.invalidDesiredCapacity(jobId, targetDesired, jobProcesses);
            }
        }


        if (scalingPolicy.getMin() < scalingPolicy.getDesired()) {
            boolean succeeded = super.killWorker(taskId, user, "shrinking job", true);
            if (succeeded && logger.isDebugEnabled()) {
                logger.debug("Job state after terminate and shrink: {}", JobMgrUtils.report(jobMetadata));
            }
            return succeeded;
        }
        throw new JobUpdateException("Cannot shrink job " + jobMetadata.getJobId() +
                " as its desired size would be smaller then min=" + scalingPolicy.getMin());
    }

    @Override
    protected void decrementJobSize(V2JobMetadata jobMetadata, String user) throws InvalidJobException, IOException {
        V2StageMetadata stageMetadata = jobMetadata.getStageMetadata(1);
        StageScalingPolicy scalingPolicy = stageMetadata.getScalingPolicy();
        updateInstanceCountOnly(scalingPolicy.getMin(), scalingPolicy.getDesired() - 1, scalingPolicy.getMax(), user);

        V2StageMetadataWritable wSM = (V2StageMetadataWritable) stageMetadata;
        wSM.unsafeSetNumWorkers(wSM.getNumWorkers() - 1);
        store.updateStage(wSM);
    }

    private StageSchedulingInfo getSchedulingInfo() {
        return jobDefinition.getSchedulingInfo().getStages().values().iterator().next();
    }

    private StageScalingPolicy getScalingPolicy() {
        return getSchedulingInfo().getScalingPolicy();
    }

    @Override
    public void updateInstances(int stageNum, int min, int desired, int max, String user) throws InvalidJobException {
        final V2JobMetadataWritable jobMetadata = (V2JobMetadataWritable) getJobMetadata();

        try (AutoCloseable l = jobMetadata.obtainLock()) {
            logger.info("Updating instance counts: min={}, desired={}, max={}, user={}", min, desired, max, user);

            long startTime = System.currentTimeMillis();
            V2StageMetadataWritable serviceStage = (V2StageMetadataWritable) jobMetadata.getStageMetadata(stageNum);
            StageScalingPolicy scalingPolicy = serviceStage.getScalingPolicy();

            // check job processes
            ServiceJobProcesses jobProcesses = serviceStage.getJobProcesses();
            if (jobProcesses != null) {
                if (isTargetDesiredCountInvalid(desired, scalingPolicy, jobProcesses)) {
                    throw JobManagerException.invalidDesiredCapacity(jobId, desired, jobProcesses);
                }
            }

            updateInstanceCounts(min, desired, max, user, scalingPolicy.getDesired());
            logger.info("Scaling policy updated in {}[ms]", System.currentTimeMillis() - startTime);
        } catch (JobManagerException e) {
            logger.warn(jobId + ": JobManagerException in updating instance counts - " + e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.warn(jobId + ": unexpected exception locking job metadata: " + e.getMessage(), e);
            throw new InvalidJobException(jobId, e);
        }
    }

    public void updateJobProcesses(int stageNum, boolean disableIncreaseDesired, boolean disableDecreaseDesired, String user) throws InvalidJobException {
        final V2JobMetadataWritable jobMetadata = (V2JobMetadataWritable) getJobMetadata();
        try (AutoCloseable ignored = jobMetadata.obtainLock()) {
            V2StageMetadataWritable serviceStage = (V2StageMetadataWritable) jobMetadata.getStageMetadata(stageNum);
            ServiceJobProcesses jobProcesses = ServiceJobProcesses.newBuilder().withDisableIncreaseDesired(disableIncreaseDesired).withDisableDecreaseDesired(disableDecreaseDesired).build();
            serviceStage.setJobProcesses(jobProcesses);
            store.updateStage(serviceStage);
            logger.info("{} : Updated JobProcesses disableIncreaseDesired={}, disableDecreaseDesired={}, user={}", jobId, disableIncreaseDesired, disableDecreaseDesired, user);

            auditLogService.submit(
                    new AuditLogEvent(AuditLogEvent.Type.JOB_PROCESSES_UPDATE, jobId,
                            String.format("disableIncreaseDesired=%s, disableDecreaseDesired=%s, user=%s", disableIncreaseDesired, disableDecreaseDesired, user),
                            System.currentTimeMillis()));
            if (disableDecreaseDesired) {
                eventBus.publish(new JobStateChangeEvent<>(jobId, JobState.DisabledDecreaseDesired, System.currentTimeMillis(), jobMetadata));
            }
            if (disableIncreaseDesired) {
                eventBus.publish(new JobStateChangeEvent<>(jobId, JobState.DisabledIncreaseDesired, System.currentTimeMillis(), jobMetadata));
            }

        } catch (Exception e) {
            logger.warn(jobId + ": unexpected exception locking job metadata: " + e.getMessage(), e);
            throw new InvalidJobException(jobId, e);
        }
    }

    private void updateInstanceCounts(int min, int desired, int max, String user, int oldDesired) throws InvalidJobException {
        StageScalingPolicy newPolicy = updateInstanceCountOnly(min, desired, max, user);
        setCurrentInstances(oldDesired, newPolicy.getDesired(), user);
    }

    protected StageScalingPolicy updateInstanceCountOnly(int min, int desired, int max, String user) throws InvalidJobException {
        if (!isActive()) {
            throw new InvalidJobException(jobId, new Exception("Job not active"));
        }
        final V2JobMetadataWritable jobMetadata = (V2JobMetadataWritable) getJobMetadata();
        try (AutoCloseable l = jobMetadata.obtainLock()) {

            V2StageMetadataWritable serviceStage = (V2StageMetadataWritable) jobMetadata.getStageMetadata(1);
            StageScalingPolicy scalingPolicy = serviceStage.getScalingPolicy();

            int effMin = min >= 0 ? min : scalingPolicy.getMin();
            int effDesired = desired >= 0 ? desired : scalingPolicy.getDesired();
            int effMax = max >= 0 ? max : scalingPolicy.getMax();
            if (effMin > effDesired) {
                throw new InvalidJobException(jobId + ": invalid min=" + effMin + " > desired=" + effDesired);
            }
            if (effDesired > effMax) {
                throw new InvalidJobException(jobId + ": invalid desired=" + effDesired + " > max=" + effMax);
            }

            StageScalingPolicy newScalingPolicy = new StageScalingPolicy(1, effMin, effMax, effDesired,
                    scalingPolicy.getIncrement(), scalingPolicy.getDecrement(), scalingPolicy.getCoolDownSecs(),
                    scalingPolicy.getStrategies()
            );
            serviceStage.setScalingPolicy(newScalingPolicy);

            store.updateJob(jobId, jobMetadata);
            logger.info(jobId + ": setting instances min=" + effMin + ", max=" + effMax + ", desired=" + effDesired);

            auditLogService.submit(
                    new AuditLogEvent(AuditLogEvent.Type.JOB_SCALE_UPDATE, jobId,
                            "min=" + effMin + ", max=" + effMax + ", desired=" + effDesired + ", user=" + user, System.currentTimeMillis()));
            eventBus.publish(new JobStateChangeEvent<>(jobId, JobState.Resized, System.currentTimeMillis(), jobMetadata));

            return newScalingPolicy;
        } catch (InvalidJobException e) {
            logger.warn(jobId + ": couldn't create new tasks after setting instance counts: " + e.getMessage(), e);
            throw new InvalidJobException(jobId,
                    new Exception("Couldn't create new tasks after setting instance counts: " + e.getMessage()));
        } catch (Exception e) {
            logger.warn(jobId + ": unexpected exception locking job metadata: " + e.getMessage(), e);
            throw new InvalidJobException(jobId, e);
        }
    }

    @Override
    public void setProcessStatus_TO_BE_RENAMED(int stage, boolean inService, String user) throws InvalidJobException {
        if (!isActive()) {
            throw new InvalidJobException(jobId, new Exception("Job not active"));
        }
        final V2JobMetadataWritable jobMetadata = (V2JobMetadataWritable) getJobMetadata();
        try (AutoCloseable l = jobMetadata.obtainLock()) {

            jobMetadata.setParameters(Parameters.updateInService(jobDefinition.getParameters(), inService));
            store.updateJob(jobId, jobMetadata);

            logger.info(jobId + ": changing inService status to {}", inService);
            auditLogService.submit(
                    new AuditLogEvent(AuditLogEvent.Type.JOB_IN_SERVICE_STATUS_CHANGE, jobId,
                            "inService=" + inService, System.currentTimeMillis()));

            JobState jobState = inService ? JobState.Activated : JobState.Deactivated;
            eventBus.publish(new JobStateChangeEvent<>(jobId, jobState, System.currentTimeMillis(), jobMetadata));
        } catch (InvalidJobException e) {
            logger.warn(jobId + ": couldn't create new tasks after setting inService status: " + e.getMessage(), e);
            throw new InvalidJobException(jobId,
                    new Exception("Couldn't create new tasks after setting inService status: " + e.getMessage()));
        } catch (Exception e) {
            logger.warn(jobId + ": unexpected exception locking job metadata: " + e.getMessage(), e);
            throw new InvalidJobException(jobId, e);
        }
    }

    private void setCurrentInstances(int oldDesired, int desired, String user)
            throws InvalidJobException {
        // expect job metadata to be already locked
        try {
            if (oldDesired < desired) {
                createScaledUpWorkers(oldDesired, desired, user);
            } else if (oldDesired > desired) {
                removeScaledDownWorkers(oldDesired, desired, user);
            }
        } catch (IOException e) {
            logger.error(jobId + ": unexpected from store during setting desired instances from " + oldDesired +
                    " to " + desired, e);
            throw new InvalidJobException("Unexpected error from Store: " + e.getMessage());
        } catch (InvalidJobException e) {
            throw new InvalidJobException(e.getMessage(), e);
        }
    }

    private void createScaledUpWorkers(int oldCount, int newCount, String user)
            throws IOException, InvalidJobException {
        // expect job metadata to be already locked
        final V2StageMetadataWritable stage = (V2StageMetadataWritable) getJobMetadata().getStageMetadata(1);
        stage.unsafeSetNumWorkers(newCount);
        store.updateStage(stage);
        createAllWorkersIntl(oldCount, newCount, newCount);
    }

    private void removeScaledDownWorkers(int oldCount, int newCount, String user) throws IOException, InvalidJobException {
        // expect job metadata to be already locked
        final V2StageMetadataWritable stage = (V2StageMetadataWritable) getJobMetadata().getStageMetadata(1);
        List<V2WorkerMetadataWritable> removed = new ArrayList<>();

        int oldCountAdjusted = JobMgrUtils.getAdjustedWorkerNum(stage, oldCount, true); // == oldCount + interleaved tomb stones
        int newCountAdjusted = JobMgrUtils.getAdjustedWorkerNum(stage, newCount, false); // == newCount + interleaved tomb stones

        for (int i = oldCountAdjusted - 1; i >= newCountAdjusted; i--) {
            try {
                final V2WorkerMetadata w = stage.getWorkerByIndex(i);
                if (!JobMgrUtils.isTombStoned(w)) {
                    killWorker(w, user, "scaling down job", false, false);
                }
                if (!stage.unsafeRemoveWorker(w.getWorkerIndex(), w.getWorkerNumber())) {
                    logger.warn(jobId + ": Unexpected to not remove worker index " + i + " when scaling down from " +
                            oldCount + " to " + newCount);
                } else {
                    removed.add((V2WorkerMetadataWritable) w);
                }
            } catch (InvalidJobException e) {
                logger.warn(jobId + ": Unexpected to not find worker index " + i + " to scale down job from " +
                        oldCount + " to " + newCount);
            }
        }
        for (V2WorkerMetadataWritable w : removed) {
            store.archiveWorker(w);
        }
        stage.unsafeSetNumWorkers(newCount);
        store.updateStage(stage);
    }

    @Override
    protected void createAllWorkers() throws InvalidJobException {
        int desired = getScalingPolicy().getDesired();
        createAllWorkersIntl(0, desired, desired);
    }

    private void createAllWorkersIntl(int from, int to, int numInstances)
            throws InvalidJobException {
        if (logger.isDebugEnabled()) {
            logger.debug("Job state={}", JobMgrUtils.report(getJobMetadata()));
        }
        List<Integer> tombStoneSlots = JobMgrUtils.getTombStoneSlots(getJobMetadata());

        int total = to - from;
        int tombStonePart = Math.min(total, tombStoneSlots.size());
        for (int i = 0; i < tombStonePart; i++) {
            Integer idx = tombStoneSlots.get(i);

            V2WorkerMetadata idxWorker;
            try {
                idxWorker = getJobMetadata().getStageMetadata(1).getWorkerByIndex(idx);
            } catch (InvalidJobException ignore) {
                idxWorker = null;
            }

            if (idxWorker == null) { // Tombstone task, after loading from storage workerByIndex is null
                V2StageMetadata stageMetadata = getJobMetadata().getStageMetadata(1);
                for (V2WorkerMetadata worker : stageMetadata.getAllWorkers()) {
                    if (worker.getWorkerIndex() == idx) {
                        idxWorker = worker;
                        break;
                    }
                }
            }
            if (idxWorker == null) { // We have data inconsistency
                throw new InvalidJobException(getJobId(), new IllegalStateException("Inconsistent data"));
            }

            try {
                logger.debug("Replacing tombstone for job {} at index {}", jobId, idxWorker.getWorkerIndex());
                replaceWorker(idxWorker);
            } catch (Exception e) {
                throw new InvalidJobException(getJobId(), new IllegalStateException("Cannot recycle worker slot with tombstone", e));
            }
        }
        if (tombStonePart == total) {
            return;
        }

        List<WorkerRequest> workers = getWorkersInRange(from + tombStonePart, to, numInstances);
        if (workers == null || workers.isEmpty()) {
            return;
        }
        int beg = 0;
        while (beg < workers.size()) {
            int en = beg + Math.min(workerWritesBatchSize, workers.size() - beg);
            storeNewWorkers(workers.subList(beg, en));
            beg = en;
        }
    }

    private void storeNewWorkers(List<WorkerRequest> workers) throws InvalidJobException {
        try {
            List<? extends V2WorkerMetadata> newWorkers = store.storeNewWorkers(workers);
            newWorkers.forEach(w -> {
                queueTask(w);
                jobMetrics.updateTaskMetrics(w);
            });
        } catch (InvalidJobException e) {
            // TODO confirm if this is possible, likely not
            throw new InvalidJobException("Unexpected error: " + e.getMessage(), e);
        } catch (IOException e) {
            logger.error("Error storing workers of job " + jobId + " - " + e.getMessage(), e);
        }
    }

    private boolean isTargetDesiredCountInvalid(int targetDesired, StageScalingPolicy stageScalingPolicy, ServiceJobProcesses jobProcesses) {
        return (jobProcesses.isDisableIncreaseDesired() && targetDesired > stageScalingPolicy.getDesired()) ||
                (jobProcesses.isDisableDecreaseDesired() && targetDesired < stageScalingPolicy.getDesired());
    }
}
