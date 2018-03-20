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

package io.netflix.titus.master.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.inject.Injector;
import com.netflix.fenzo.PreferentialNamedConsumableResourceSet.ConsumeResult;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.queues.QueuableTask;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import io.netflix.titus.api.audit.model.AuditLogEvent;
import io.netflix.titus.api.audit.service.AuditLogService;
import io.netflix.titus.api.jobmanager.model.job.sanitizer.JobConfiguration;
import io.netflix.titus.api.model.event.JobStateChangeEvent;
import io.netflix.titus.api.model.event.JobStateChangeEvent.JobState;
import io.netflix.titus.api.model.event.TaskStateChangeEvent;
import io.netflix.titus.api.model.v2.JobCompletedReason;
import io.netflix.titus.api.model.v2.ServiceJobProcesses;
import io.netflix.titus.api.model.v2.V2JobDefinition;
import io.netflix.titus.api.model.v2.V2JobState;
import io.netflix.titus.api.model.v2.WorkerAssignments;
import io.netflix.titus.api.model.v2.WorkerHost;
import io.netflix.titus.api.model.v2.WorkerNaming;
import io.netflix.titus.api.model.v2.descriptor.SchedulingInfo;
import io.netflix.titus.api.model.v2.descriptor.StageSchedulingInfo;
import io.netflix.titus.api.model.v2.parameter.Parameters;
import io.netflix.titus.api.store.v2.InvalidJobException;
import io.netflix.titus.api.store.v2.V2JobMetadata;
import io.netflix.titus.api.store.v2.V2StageMetadata;
import io.netflix.titus.api.store.v2.V2WorkerMetadata;
import io.netflix.titus.common.util.rx.eventbus.RxEventBus;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.JobSchedulingInfo;
import io.netflix.titus.master.Status;
import io.netflix.titus.master.config.CellInfoResolver;
import io.netflix.titus.master.config.MasterConfiguration;
import io.netflix.titus.master.job.worker.WorkerNumberGenerator;
import io.netflix.titus.master.job.worker.WorkerRequest;
import io.netflix.titus.master.job.worker.WorkerResubmitRateLimiter;
import io.netflix.titus.master.mesos.TitusTaskInfoCreator;
import io.netflix.titus.master.model.job.TitusQueuableTask;
import io.netflix.titus.master.scheduler.ScheduledRequest;
import io.netflix.titus.master.scheduler.SchedulingService;
import io.netflix.titus.master.service.management.ApplicationSlaManagementService;
import io.netflix.titus.master.store.InvalidJobStateChangeException;
import io.netflix.titus.master.store.JobAlreadyExistsException;
import io.netflix.titus.master.store.NamedJob;
import io.netflix.titus.master.store.V2JobStore;
import io.netflix.titus.master.store.V2StageMetadataWritable;
import io.netflix.titus.master.store.V2WorkerMetadataWritable;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import rx.Observable;
import rx.Observer;
import rx.functions.Action1;
import rx.observers.SerializedObserver;
import rx.subjects.ReplaySubject;

public abstract class BaseJobMgr implements V2JobMgrIntf {

    private static final ObjectMapper mapper;

    static {
        mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private final AuditLogService auditLogService;
    private final MasterConfiguration config;
    private final CellInfoResolver cellInfoResolver;
    private final JobConfiguration jobConfiguration;
    private final Logger logger;
    protected final String jobId;
    protected final NamedJob namedJob;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    protected final V2JobDefinition jobDefinition;
    protected final V2JobMetrics jobMetrics;
    private final ExcludedAgentsTracker excludedAgentsTracker;
    protected V2JobStore store = null;
    private Consumer<String> taskKillAction = null;
    private final WorkerNumberGenerator workerNumberGenerator;
    private volatile ScheduledFuture<?> enforceSlaFuture = null;
    private final SerializedObserver<Status> statusSerializedObserver;
    private final ReplaySubject<Status> statusReplaySubject;
    private final Counter numWorkerResubmissions;
    protected final Counter numWorkerResubmitLimitReached;
    private final Counter numWorkerTerminated;
    private final Counter numJobTerminated;
    private final Counter workerLaunchToStartMillis;
    protected final WorkerResubmitRateLimiter resubmitRateLimiter;
    private final JobAssignmentsPublisher assignmentsPublisher;
    protected final int workerWritesBatchSize;
    protected final RxEventBus eventBus;
    protected Action1<QueuableTask> taskQueueAction;
    private final SchedulingService schedulingService;
    protected final Injector injector;
    private final ApplicationSlaManagementService applicationSlaManagementService;
    private final TitusTaskInfoCreator titusTaskInfoCreator;

    private volatile boolean pendingInitialization = false;
    private final BlockingQueue<ScheduledRequest> taskQueueRequests = new LinkedBlockingQueue<>();

    public BaseJobMgr(Injector injector,
                      String jobId,
                      boolean serviceJob,
                      V2JobDefinition jobDefinition,
                      NamedJob namedJob,
                      Observer<Observable<JobSchedulingInfo>> jobSchedulingObserver,
                      MasterConfiguration config,
                      CellInfoResolver cellInfoResolver,
                      JobManagerConfiguration jobManagerConfiguration,
                      JobConfiguration jobConfiguration,
                      Logger logger,
                      String rootMetricsName,
                      RxEventBus eventBus,
                      SchedulingService schedulingService,
                      Registry registry) {
        this.injector = injector;
        applicationSlaManagementService = injector.getInstance(ApplicationSlaManagementService.class);
        resubmitRateLimiter = injector.getInstance(WorkerResubmitRateLimiter.class);
        auditLogService = injector.getInstance(AuditLogService.class);
        this.config = config;
        this.jobConfiguration = jobConfiguration;
        this.cellInfoResolver = cellInfoResolver;
        this.logger = logger;
        this.jobId = jobId;
        this.namedJob = namedJob;
        this.jobDefinition = jobDefinition;
        this.workerWritesBatchSize = config.getWorkerWriteBatchSize();
        this.eventBus = eventBus;
        this.schedulingService = schedulingService;
        taskQueueAction = schedulingService.getTaskQueueAction();

        String appName = Parameters.getAppName(jobDefinition.getParameters());
        if (Strings.isNullOrEmpty(appName)) {
            appName = "UNKNOWN";
        }
        String capacityGroup = Parameters.getCapacityGroup(jobDefinition.getParameters());
        if (Strings.isNullOrEmpty(capacityGroup)) {
            capacityGroup = "UNKNOWN";
        }
        this.jobMetrics = new V2JobMetrics(jobId, serviceJob, appName, capacityGroup, registry);
        this.excludedAgentsTracker = new ExcludedAgentsTracker(jobId, appName, jobManagerConfiguration, registry);

        workerNumberGenerator = new WorkerNumberGenerator(jobId);

        numWorkerResubmissions = registry.counter(rootMetricsName + "numWorkerResubmissions");
        numWorkerResubmitLimitReached = registry.counter(rootMetricsName + "numWorkerResubmitLimitReached");
        numWorkerTerminated = registry.counter(rootMetricsName + "numWorkerTerminated");
        numJobTerminated = registry.counter(rootMetricsName + "numJobTerminated");
        workerLaunchToStartMillis = registry.counter(rootMetricsName + "workerLaunchToStartMillis");

        statusReplaySubject = ReplaySubject.create();  // TODO should be bounded
        statusSerializedObserver = new SerializedObserver<>(statusReplaySubject);
        assignmentsPublisher = new JobAssignmentsPublisher(jobId, jobSchedulingObserver, statusReplaySubject);

        titusTaskInfoCreator = new TitusTaskInfoCreator(config, jobConfiguration);
    }

    @Override
    public String getJobId() {
        return jobId;
    }

    @Override
    public V2JobDefinition getJobDefinition() {
        return jobDefinition;
    }

    @Override
    public V2JobMetadata getJobMetadata() {
        return store.getActiveJob(jobId);
    }

    @Override
    public V2JobMetadata getJobMetadata(boolean evenIfArchived) {
        final V2JobMetadata job = store.getActiveJob(jobId);
        try {
            return job == null ? store.getCompletedJob(jobId) : job;
        } catch (Exception e) {
            logger.warn("Error loading completed job " + jobId);
            return null;
        }
    }

    @Override
    public ReplaySubject<Status> getStatusSubject() {
        return statusReplaySubject;
    }

    @Override
    public void setTaskKillAction(Consumer<String> killAction) {
        this.taskKillAction = killAction;
    }

    public V2JobMetrics getJobMetrics() {
        return jobMetrics;
    }

    protected abstract boolean shouldResubmit(V2WorkerMetadata mwmd, V2JobState newState);

    protected abstract void createAllWorkers() throws InvalidJobException;

    @Override
    public List<? extends V2WorkerMetadata> getArchivedWorkers() {
        try {
            return store.getArchivedWorkers(jobId);
        } catch (IOException e) {
            logger.error("Can't get archived workers - " + e.getMessage(), e);
        }
        return new ArrayList<>();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<? extends V2WorkerMetadata> getWorkers() {
        V2JobMetadata jobMetadata = getJobMetadata();
        if (jobMetadata == null) {
            return Collections.emptyList();
        }
        return (List<V2WorkerMetadata>) JobMgrUtils.getAllWorkers(
                jobMetadata,
                mwmd -> true,
                mwmd -> mwmd
        );
    }

    @Override
    public void updateInstances(int stageNum, int min, int desired, int max, String user) throws InvalidJobException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setProcessStatus_TO_BE_RENAMED(int stage, boolean inService, String user) throws InvalidJobException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void enforceSla() {
        // no-op
    }

    private void dequeueIfTerminalAndSendStatus(Status status) {
        try {
            if (V2JobState.isTerminalState(status.getState())) {
                final int workerNumber = status.getWorkerNumber();
                final V2JobMetadata jobMetadata = getJobMetadata();
                if (jobMetadata != null) {
                    try {
                        final V2WorkerMetadata w = jobMetadata.getWorkerByNumber(workerNumber);
                        if (w != null) {
                            schedulingService.removeTask(
                                    WorkerNaming.getWorkerName(jobId, w.getWorkerIndex(), w.getWorkerNumber()),
                                    ScheduledRequest.getQAttributes(this, applicationSlaManagementService), w.getSlave());
                        }
                    } catch (InvalidJobException e) {
                        logger.info(jobId + ": couldn't remove task number " + workerNumber +
                                " from scheduler queue upon state=" + status.getState() + " - " + e.getMessage());
                    }
                }
            }
            statusSerializedObserver.onNext(status);
        } catch (Exception e) {
            logger.warn("Problem sending status out: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean isActive() {
        return !V2JobState.isTerminalState(getJobMetadata().getState());
    }

    private void storeWorkerStateAndSendStatus(int workerNumber, V2JobState state, JobCompletedReason reason, Status status)
            throws IOException, InvalidJobException, InvalidJobStateChangeException {
        dequeueIfTerminalAndSendStatus(status);
        store.storeWorkerState(jobId, workerNumber, state, reason, false);
    }

    @Override
    public boolean killTask(String taskId, String user, String reason) {
        return killWorker(taskId, user, reason, false);
    }

    protected boolean killWorker(String taskId, String user, String reason, boolean shrink) {
        final WorkerNaming.JobWorkerIdPair jobAndWorkerId = WorkerNaming.getJobAndWorkerId(taskId);
        try (AutoCloseable l = getJobMetadata().obtainLock()) {
            final V2WorkerMetadata task = getJobMetadata().getWorkerByNumber(jobAndWorkerId.workerNumber);
            if (V2JobState.isTerminalState(task.getState())) {
                return false;
            }
            killWorker(task, user, reason, true, shrink);
            if (shrink) {
                decrementJobSize(getJobMetadata(), user);
            }
            return true;
        } catch (InvalidJobException e) {
            return false;
        } catch (Exception e) {
            logger.warn(jobId + ": Unexpected error killing worker number " + jobAndWorkerId.workerNumber + ", requested by user " + user);
            return false;
        }
    }

    protected void decrementJobSize(V2JobMetadata jobMetadata, String user) throws InvalidJobException, IOException {
        // No-op by default
    }

    protected void killWorker(V2WorkerMetadata task, String user, String reasonString, boolean terminateJobIfLastWorker, boolean shrink) {
        // TODO use reasonString appropriately
        try (AutoCloseable l = getJobMetadata().obtainLock()) {
            boolean killJob = false;
            if (terminateJobIfLastWorker) {
                final List<V2WorkerMetadata> allActiveWorkers = getAllActiveWorkers(getJobMetadata());
                if (!shrink && allActiveWorkers.size() == 1 && allActiveWorkers.get(0).getWorkerNumber() == task.getWorkerNumber()) {
                    killJob = true;
                }
            }

            boolean isRunning = V2JobState.isRunningState(task.getState());

            if (!V2JobState.isTerminalState(task.getState())) {
                JobCompletedReason reason = shrink ? JobCompletedReason.TombStone : JobCompletedReason.Killed;
                if (reason == JobCompletedReason.TombStone) {
                    logger.debug("Setting tombstone on {}", WorkerNaming.getWorkerName(jobId, task.getWorkerIndex(), task.getWorkerNumber()));
                }
                final Status status = new Status(jobId, WorkerNaming.getTaskId(task), task.getStageNum(), task.getWorkerIndex(),
                        task.getWorkerNumber(), Status.TYPE.INFO,
                        "task " + task.getWorkerIndex() + "number " + task.getWorkerNumber() +
                                " Killed", Status.getDataStringFromObj(task.getStatusData()), V2JobState.Failed);
                status.setReason(reason);
                storeWorkerStateAndSendStatus(task.getWorkerNumber(), V2JobState.Failed, reason, status);
                numWorkerTerminated.increment();
                String taskId = WorkerNaming.getWorkerName(jobId, task.getWorkerIndex(), task.getWorkerNumber());
                AuditLogEvent auditLogEvent = AuditLogEvent.of(AuditLogEvent.Type.WORKER_TERMINATE,
                        taskId, "stage=" + task.getStageNum() +
                                ", index=" + task.getWorkerIndex() + ", user=" + user, task);
                auditLogService.submit(auditLogEvent);
                eventBus.publish(new TaskStateChangeEvent<>(jobId, taskId, task.getState(), System.currentTimeMillis(), Pair.of(getJobMetadata(), task)));
            }
            // tell vmService to kill it anyway, just in case it is still running
            if (isRunning) {
                taskKillAction.accept(WorkerNaming.getWorkerName(task.getJobId(), task.getWorkerIndex(), task.getWorkerNumber()));
            }
            if (terminateJobIfLastWorker && killJob) {
                storeAndMarkJobTerminated(V2JobState.Completed, "All tasks completed after user " + user +
                        user + " killed worker number " + task.getWorkerNumber());
            }
        } catch (IOException e) {
            logger.error("Can't kill worker " + task.getWorkerNumber() + " of job " + jobId + ": " + e.getMessage());
        } catch (Exception e) {
            logger.warn("Unexpected error killing worker " + task.getWorkerNumber() + " of job " +
                    jobId + ": " + e.getMessage(), e);
        } finally {
            jobMetrics.updateTaskMetrics(task);
        }
    }

    @Override
    public void killJob(String user, String reason) {
        V2JobMetadata jobMetadata = getJobMetadata();
        try (AutoCloseable l = jobMetadata.obtainLock()) {
            if (V2JobState.isTerminalState(jobMetadata.getState())) {
                return;
            }
            for (V2WorkerMetadata mwmd : getAllActiveWorkers(jobMetadata)) {
                if (V2JobState.isRunningState(mwmd.getState()) || V2JobState.Accepted == mwmd.getState()) {
                    killWorker(mwmd, user, "due to job kill", false, false);
                }
            }
            storeAndMarkJobTerminated(V2JobState.Failed, "killed by " + user + ": " + reason);
            if (reason != null && !reason.isEmpty()) {
                dequeueIfTerminalAndSendStatus(new Status(jobId, null, -1, -1, -1, Status.TYPE.INFO, user + " killing job, reason: " + reason,
                        null, V2JobState.Completed));
                logger.info("Killing job " + jobId + ": " + reason);
            }
        } catch (Exception e) {
            logger.error("Unexpected error while killing job: " + e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    protected List<V2WorkerMetadata> getAllActiveWorkers(V2JobMetadata job) {
        return (List<V2WorkerMetadata>) JobMgrUtils.getAllWorkers(job,
                task -> !V2JobState.isTerminalState(task.getState()), task -> task);
    }

    @Override
    public void handleStatus(final Status status) {
        if (status.getWorkerNumber() == -1) {
            logger.warn(jobId + ": Status on invalid worker number -1, ignoring (status: " + status.toString() + ")");
            return;
        }
        final V2JobMetadata job = getJobMetadata();
        V2WorkerMetadata task = null;
        try (AutoCloseable l = job.obtainLock()) {
            try {
                task = job.getWorkerByNumber(status.getWorkerNumber());
            } catch (InvalidJobException e) {
                logger.warn(jobId + ": Unexpected to not find task index " + status.getWorkerIndex() + " number "
                        + status.getWorkerNumber() + " for status update state=" + status.getState() + ", killing the task");
                if (status.getReason() == JobCompletedReason.Normal) {
                    taskKillAction.accept(WorkerNaming.getWorkerName(jobId, status.getWorkerIndex(), status.getWorkerNumber()));
                }
                return;
            }
            if (task.getState() == status.getState()) {
                // Due to race condition, emit event for 'Launched' state always.
                if (task.getState() == V2JobState.Launched) {
                    auditLogService.submit(new AuditLogEvent(AuditLogEvent.Type.WORKER_START,
                            WorkerNaming.getWorkerName(jobId, status.getWorkerIndex(), status.getWorkerNumber()),
                            "started", System.currentTimeMillis()));
                }
                return; // no-op
            }
            excludedAgentsTracker.update(task, status.getState(), status.getReason());
            if (V2JobState.isTerminalState(task.getState()) && !V2JobState.isTerminalState(status.getState())) {
                logger.info(jobId + " " + getTaskStringPrefix(task.getWorkerIndex(), task.getWorkerNumber()) +
                        " killing terminal task reporting state " + status.getState());
                taskKillAction.accept(WorkerNaming.getWorkerName(jobId, task.getWorkerIndex(), task.getWorkerNumber()));
            }
            if (!task.getState().isValidStateChgTo(status.getState())) {
                logger.warn(jobId + ": Ignoring invalid state change for task index=" + task.getWorkerIndex() +
                        " number " + task.getWorkerNumber() + " from " + task.getState() + " to " + status.getState());
                return;
            }
            if (task.getState() == V2JobState.Failed && task.getReason() == JobCompletedReason.TombStone) {
                ((V2WorkerMetadataWritable) task).setCompletionMessage(status.getMessage());
                return;
            }
            boolean shdResubmit = shouldResubmit(task, status.getState());
            try {
                if (V2JobState.isTerminalState(status.getState())) {
                    ((V2WorkerMetadataWritable) task).setCompletionMessage(status.getMessage());
                }
                if (V2JobState.isTerminalState(status.getState()) && shdResubmit) {

                    String reason = "Replacing failed task number " + status.getWorkerNumber();
                    if (!Strings.isNullOrEmpty(status.getMessage())) {
                        reason = status.getMessage() + ". " + reason;
                    }

                    V2WorkerMetadata rt = resubmitWorker(status, workerNumberGenerator.getNextWorkerNumber(),
                            WorkerRequest.generateWorkerInstanceId(), reason);

                    if (rt != null) {
                        String taskId = WorkerNaming.getWorkerName(jobId, rt.getWorkerIndex(), rt.getWorkerNumber());
                        AuditLogEvent auditLogEvent = AuditLogEvent.of(AuditLogEvent.Type.WORKER_TERMINATE,
                                taskId, "index=" + status.getWorkerIndex() + ", resubmitted", rt);
                        auditLogService.submit(auditLogEvent);
                        eventBus.publish(new TaskStateChangeEvent<>(jobId, taskId, task.getState(), System.currentTimeMillis(), Pair.of(job, rt)));
                    }
                } else {
                    boolean isLastTaskOfJob = false;
                    List<V2WorkerMetadata> allActiveWorkers = getAllActiveWorkers(job);
                    if (allActiveWorkers != null && allActiveWorkers.size() == 1) {
                        if (allActiveWorkers.get(0).getWorkerNumber() == status.getWorkerNumber()) {
                            // got complete on the last running worker, terminate job
                            isLastTaskOfJob = true;
                        }
                    }
                    try {
                        if (status.getData() != null && !status.getData().isEmpty()) {
                            ((V2WorkerMetadataWritable) task).setStatusData(mapper.readValue(status.getData(), Object.class));
                        }
                    } catch (IOException e) {
                        logger.warn(jobId + ": worker " + task.getWorkerNumber() + ": Error parsing status data as json: " + e.getMessage());
                    }
                    storeWorkerStateAndSendStatus(task.getWorkerNumber(), status.getState(), status.getReason(), status);
                    if (V2JobState.isTerminalState(status.getState())) {
                        String taskId = WorkerNaming.getWorkerName(jobId, task.getWorkerIndex(), task.getWorkerNumber());
                        AuditLogEvent auditLogEvent = AuditLogEvent.of(AuditLogEvent.Type.WORKER_TERMINATE, taskId,
                                "index=" + status.getWorkerIndex() + ", reason=" + status.getReason() + ", not resubmitted", task);
                        auditLogService.submit(auditLogEvent);
                        eventBus.publish(new TaskStateChangeEvent<>(jobId, taskId, task.getState(), System.currentTimeMillis(), Pair.of(job, task)));

                        // explicitly kill the worker reporting complete
                        taskKillAction.accept(WorkerNaming.getWorkerName(jobId, status.getWorkerIndex(), status.getWorkerNumber()));
                        if (isLastTaskOfJob) {
                            storeAndMarkJobTerminated(V2JobState.Completed, "all tasks completed");
                        }
                    } else if (status.getState() == V2JobState.Started) {
                        if (job.getState() == V2JobState.Accepted) {
                            store.storeJobState(jobId, V2JobState.Launched);
                        }
                        auditLogService.submit(new AuditLogEvent(AuditLogEvent.Type.WORKER_START,
                                WorkerNaming.getWorkerName(jobId, status.getWorkerIndex(), status.getWorkerNumber()),
                                "started", System.currentTimeMillis()));

                        String taskId = WorkerNaming.getWorkerName(jobId, task.getWorkerIndex(), task.getWorkerNumber());
                        eventBus.publish(new TaskStateChangeEvent<>(jobId, taskId, task.getState(), System.currentTimeMillis(), Pair.of(job, task)));

                        workerLaunchToStartMillis.increment(task.getStartedAt() - task.getLaunchedAt());
                    } else if (status.getState() == V2JobState.StartInitiated) {
                        eventBus.publish(new TaskStateChangeEvent<>(jobId, WorkerNaming.getTaskId(task), task.getState(), System.currentTimeMillis(), Pair.of(job, task)));
                    }
                }
            } catch (IOException e) {
                logger.warn("Can't store task state for job {}: {}", jobId, e.getMessage(), e);
                return;
            } catch (InvalidJobException | InvalidJobStateChangeException e) {
                logger.warn(jobId + ": Unexpected error storing task state to " + status.getState() + ": " + e.getMessage());
                return;
            }
        } catch (Exception e) {
            logger.error(jobId + ": Unexpected error locking job object: " + e.getMessage());
        } finally {
            if (task != null) {
                jobMetrics.updateTaskMetrics(task);
            }
        }
    }

    protected String getTaskStringPrefix(int index, int number) {
        return "task index=" + index + " number=" + number;
    }

    @Override
    public Protos.TaskInfo setLaunchedAndCreateTaskInfo(TitusQueuableTask task, String hostname,
                                                        Map<String, String> attributeMap,
                                                        Protos.SlaveID slaveID,
                                                        ConsumeResult consumedResourceSet,
                                                        List<Integer> portsAssigned)
            throws InvalidJobStateChangeException, InvalidJobException {
        List<V2WorkerMetadata.TwoLevelResource> twoLevelResources = new ArrayList<>();
        if (consumedResourceSet != null) {
            logger.info(task.getId() + ": allocated res " + consumedResourceSet.getAttrName() + ", " +
                    consumedResourceSet.getResName() + ", label " + consumedResourceSet.getIndex()
            );
            twoLevelResources.add(new V2WorkerMetadata.TwoLevelResource(
                    consumedResourceSet.getAttrName(),
                    consumedResourceSet.getResName(),
                    "" + consumedResourceSet.getIndex()
            ));
        }

        V2JobMetadata job = getJobMetadata();
        final WorkerNaming.JobWorkerIdPair jobAndWorkerId = WorkerNaming.getJobAndWorkerId(task.getId());
        V2WorkerMetadataWritable mwmd = JobMgrUtils.getV2WorkerMetadataWritable(job, jobAndWorkerId.workerNumber);
        try (AutoCloseable l = job.obtainLock()) {
            mwmd.setSlave(hostname);
            mwmd.setSlaveID(slaveID.getValue());
            mwmd.setTwoLevelResources(twoLevelResources);
            mwmd.setSlaveAttributes(attributeMap);
            mwmd.setMetricsPort(0);
            mwmd.setDebugPort(0);
            mwmd.addPorts(portsAssigned);
            Status status = new Status(jobId, WorkerNaming.getTaskId(mwmd),
                    mwmd.getStageNum(), mwmd.getWorkerIndex(), mwmd.getWorkerNumber(), Status.TYPE.INFO,
                    getTaskStringPrefix(mwmd.getWorkerIndex(), mwmd.getWorkerNumber()) +
                            " scheduled on " + hostname + " with ports=" + mapper.writeValueAsString(portsAssigned)
                    , Status.getDataStringFromObj(mwmd.getStatusData()), V2JobState.Launched);
            String instanceId = attributeMap.get("id");
            if (instanceId != null) {
                status.setInstanceId(instanceId);
            }
            status.setHostname(hostname);
            storeWorkerStateAndSendStatus(jobAndWorkerId.workerNumber, V2JobState.Launched, JobCompletedReason.Normal, status);
            logger.info("Launched worker " + mwmd.getWorkerNumber() + " index " + mwmd.getWorkerIndex() +
                    " of job " + jobId + " on host " + hostname + " using ports " + mapper.writeValueAsString(portsAssigned));
        } catch (InvalidJobException | InvalidJobStateChangeException e) {
            throw e;
        } catch (Exception e) {
            throw new InvalidJobStateChangeException(jobId, mwmd.getState(), V2JobState.Launched, e);
        } finally {
            jobMetrics.updateTaskMetrics(mwmd);
        }

        TaskRequest.AssignedResources assignedResources = new TaskRequest.AssignedResources();
        assignedResources.setConsumedNamedResources(Collections.singletonList(consumedResourceSet));
        task.setAssignedResources(assignedResources);

        // create taskInfo object
        return titusTaskInfoCreator.createTitusTaskInfo(slaveID, job.getParameters(), task, portsAssigned, mwmd.getWorkerInstanceId(), attributeMap);
    }

    /**
     * Kill current task, and run a replacement.
     */
    @Override
    public void resubmitWorker(String taskId, String reason) throws InvalidJobException, InvalidJobStateChangeException {
        final WorkerNaming.JobWorkerIdPair jobAndWorkerId = WorkerNaming.getJobAndWorkerId(taskId);
        int taskNumber = jobAndWorkerId.workerNumber;
        final V2JobMetadata job = getJobMetadata();
        try (AutoCloseable l = job.obtainLock()) {
            V2WorkerMetadata task;
            try {
                task = job.getWorkerByNumber(taskNumber);
            } catch (InvalidJobException e) {
                logger.warn(jobId + ": Unexpected to not find task number " + taskNumber + " for resubmit request, ignoring");
                return;
            }
            if (V2JobState.isTerminalState(task.getState())) {
                logger.warn(jobId + ": Can't resubmit task index " + task.getWorkerIndex() + " number " +
                        task.getWorkerNumber() + ": already in " + task.getState() + " state");
                throw new InvalidJobStateChangeException(jobId, task.getState(), V2JobState.Accepted);
            }
            Status status = new Status(jobId, WorkerNaming.getTaskId(task), 1, task.getWorkerIndex(), taskNumber, Status.TYPE.INFO,
                    reason == null ? "resubmit requested" : reason, Status.getDataStringFromObj(task.getStatusData()), V2JobState.Failed);
            status.setReason(JobCompletedReason.Killed);

            try {

                String newReason = "Replacing failed task number " + status.getWorkerNumber();
                if (!Strings.isNullOrEmpty(status.getMessage())) {
                    newReason = status.getMessage() + ". " + newReason;
                }

                V2WorkerMetadata rt = resubmitWorker(status, workerNumberGenerator.getNextWorkerNumber(),
                        WorkerRequest.generateWorkerInstanceId(), newReason);
                if (rt != null) {
                    AuditLogEvent auditLogEvent = AuditLogEvent.of(AuditLogEvent.Type.WORKER_TERMINATE,
                            taskId, "index=" + task.getWorkerIndex() + ", resubmit requested", task);
                    auditLogService.submit(auditLogEvent);
                    eventBus.publish(new TaskStateChangeEvent<>(jobId, taskId, task.getState(), System.currentTimeMillis(), Pair.of(job, rt)));
                }
            } catch (InvalidJobException | InvalidJobStateChangeException e) {
                logger.warn(jobId + ": Unexpected to fail resubmit request of task index " + task.getWorkerIndex() +
                        " number " + task.getWorkerNumber());
            }
        } catch (Exception e) {
            logger.warn(jobId + ": unexpected error locking job object: " + e.getMessage());
        }
    }

    // Caller must lock appropriately
    private V2WorkerMetadata resubmitWorker(Status status, int taskNumber, String taskInstanceId, String reason) throws InvalidJobException, InvalidJobStateChangeException {
        V2JobMetadata mjmd = store.getActiveJob(jobId);
        V2WorkerMetadata mwmd = mjmd.getWorkerByNumber(status.getWorkerNumber());
        V2StageMetadata msmd = mjmd.getStageMetadata(mwmd.getStageNum());
        WorkerRequest request = new WorkerRequest(msmd.getMachineDefinition(),
                jobId, mwmd.getWorkerIndex(), taskNumber, taskInstanceId,
                cellInfoResolver.getCellName(),
                mjmd.getJarUrl(),
                mwmd.getStageNum(), mjmd.getNumStages(), msmd.getNumWorkers(),
                mjmd.getName(), msmd.getMachineDefinition().getNumPorts(),
                mjmd.getParameters(), mjmd.getSla(), msmd.getHardConstraints(), msmd.getSoftConstraints(),
                msmd.getSecurityGroups(), msmd.getAllocateIP(), jobDefinition.getSchedulingInfo(), false);
        try {

            final Status s = new Status(jobId, status.getTaskId(), mwmd.getStageNum(), status.getWorkerIndex(), status.getWorkerNumber(), Status.TYPE.INFO,
                    reason, status.getData(), status.getState());
            if (mwmd.getSlaveAttributes() != null) {
                String instanceId = mwmd.getSlaveAttributes().get("id");
                if (instanceId != null) {
                    status.setInstanceId(instanceId);
                }
            }
            s.setHostname(mwmd.getSlave());
            s.setReason(status.getReason());
            dequeueIfTerminalAndSendStatus(s);

            ((V2WorkerMetadataWritable) mwmd).setState(status.getState(), System.currentTimeMillis(), status.getReason());
            ((V2WorkerMetadataWritable) mwmd).setCompletionMessage(reason);
            taskKillAction.accept(WorkerNaming.getWorkerName(jobId, mwmd.getWorkerIndex(), mwmd.getWorkerNumber())); // in case it is still there

            AuditLogEvent taskTerminateEvent = AuditLogEvent.of(AuditLogEvent.Type.WORKER_TERMINATE, WorkerNaming.getTaskId(mwmd),
                    "index=" + status.getWorkerIndex() + ", reason=" + status.getReason(), mwmd);
            auditLogService.submit(taskTerminateEvent);
            eventBus.publish(new TaskStateChangeEvent<>(jobId, WorkerNaming.getTaskId(mwmd), mwmd.getState(), System.currentTimeMillis(), Pair.of(mjmd, mwmd)));
            final V2WorkerMetadata mwmdr = store.replaceTerminatedWorker(request, mwmd);
            queueTask(mwmdr);
            logger.info(jobId + ": Resubmitted task " + status.getWorkerNumber() + " with " + mwmdr.getWorkerNumber() +
                    " for index " + mwmd.getWorkerIndex());
            eventBus.publish(new TaskStateChangeEvent<>(jobId, WorkerNaming.getTaskId(mwmdr), mwmdr.getState(), System.currentTimeMillis(), Pair.of(mjmd, mwmdr)));
            numWorkerResubmissions.increment();

            jobMetrics.updateTaskMetrics(mwmdr);

            return mwmdr;
        } catch (IOException | InvalidJobException e) {
            logger.error("Couldn't submit replacement worker " + mwmd.getWorkerNumber() + " for job " +
                    jobId + "-worker-" + status.getWorkerIndex() + ": " + e.getMessage(), e);
            return null;
        } finally {
            jobMetrics.updateTaskMetrics(mwmd);
        }
    }

    // TODO improve the way the taskRequest is constructed
    protected void queueTask(V2WorkerMetadata mwmdr) {
        final StageSchedulingInfo schedulingInfo = jobDefinition.getSchedulingInfo().forStage(1);
        V2StageMetadata stageMetadata = getJobMetadata().getStageMetadata(mwmdr.getStageNum());
        ScheduledRequest request = createRequest(this, mwmdr,
                new WorkerRequest(schedulingInfo.getMachineDefinition(),
                        jobId, mwmdr.getWorkerIndex(), mwmdr.getWorkerNumber(), mwmdr.getWorkerInstanceId(),
                        cellInfoResolver.getCellName(), null, 1, 1,
                        schedulingInfo.getNumberOfInstances(),
                        jobDefinition.getName(), schedulingInfo.getMachineDefinition().getNumPorts(),
                        jobDefinition.getParameters(), jobDefinition.getJobSla(),
                        schedulingInfo.getHardConstraints(), schedulingInfo.getSoftConstraints(),
                        stageMetadata.getSecurityGroups(), stageMetadata.getAllocateIP(),
                        new SchedulingInfo(Collections.singletonMap(1, schedulingInfo)), false
                ));
        if (pendingInitialization) {
            taskQueueRequests.add(request);
        } else {
            taskQueueAction.call(request);
        }
    }

    private ScheduledRequest createRequest(V2JobMgrIntf jobMgr, V2WorkerMetadata mwmdr, WorkerRequest req) {
        return new ScheduledRequest(jobMgr, mwmdr, req, config, schedulingService.getV2ConstraintEvaluatorTransformer(),
                schedulingService.getSystemSoftConstraint(), schedulingService.getSystemHardConstraint(), applicationSlaManagementService);
    }

    @Override
    public V2WorkerMetadata getTask(int workerNumber, boolean evenIfArchived) {
        V2JobMetadata jobMetadata = getJobMetadata();
        if (jobMetadata == null) {
            return evenIfArchived ? tryLoadFromArchive(workerNumber) : null;
        }
        try {
            return JobMgrUtils.getV2WorkerMetadataWritable(jobMetadata, workerNumber);
        } catch (InvalidJobException e) {
            return evenIfArchived ? tryLoadFromArchive(workerNumber) : null;
        }
    }

    @Override
    public long getTaskCreateTime(String taskId) {
        final WorkerNaming.JobWorkerIdPair jobAndWorkerId = WorkerNaming.getJobAndWorkerId(taskId);
        return getTask(jobAndWorkerId.workerNumber, false).getAcceptedAt();
    }

    private V2WorkerMetadata tryLoadFromArchive(int workerNumber) {
        try {
            return store.getArchivedWorker(jobId, workerNumber);
        } catch (IOException e1) {
            logger.warn("Error getting archived worker " + workerNumber + " for job " + jobId, e1);
            return null;
        }
    }

    @Override
    public void handleTaskStuckInState(String taskId, V2JobState state) {
        final WorkerNaming.JobWorkerIdPair jobAndWorkerId = WorkerNaming.getJobAndWorkerId(taskId);
        final int taskNumber = jobAndWorkerId.workerNumber;
        final int taskIndex = jobAndWorkerId.workerIndex;
        if (state == V2JobState.Noop || V2JobState.isTerminalState(state)) {
            return;
        }
        final V2JobMetadata job = getJobMetadata();
        if (job == null) {
            return;
        }
        try (AutoCloseable wLock = job.obtainLock()) {
            final V2WorkerMetadata task = JobMgrUtils.getV2WorkerMetadataWritable(job, taskNumber);
            if (task.getState() == state) {
                logger.info(jobId + ": Will check on task " + taskIndex + "-" + taskNumber +
                        " for state " + state);
                if (state == V2JobState.Launched || state == V2JobState.StartInitiated) {
                    // resubmit worker
                    final Status status = new Status(jobId, WorkerNaming.getTaskId(task), task.getStageNum(), task.getWorkerIndex(), task.getWorkerNumber(),
                            Status.TYPE.ERROR, getTaskStringPrefix(taskIndex, taskNumber) +
                            " Resubmitting worker stuck in " + state + " state",
                            Status.getDataStringFromObj(task.getStatusData()), V2JobState.Failed);
                    status.setReason(JobCompletedReason.Lost);
                    handleStatus(status);
                }
                // Not handling worker stuck in other states for now. E.g., if stuck in Accepted state for too long
            } // else ignore
        } catch (InvalidJobException | InvalidJobStateChangeException e) {
            logger.warn(jobId + ": Unexpected error looking for task " + taskIndex + "-" + taskNumber + ", state=" +
                    state + ": " + e.getMessage());
        } catch (Exception e) {
            logger.warn(jobId + ": Unexpected error locking object - " + e.getMessage(), e);
        }
    }

    @Override
    public Set<String> getExcludedAgents() {
        return excludedAgentsTracker.getExcludedAgents();
    }

    private void storeAndMarkJobTerminated(V2JobState state, String mesg) {
        V2JobMetadata jobMetadata = null;
        try {
            jobMetadata = getJobMetadata();
            if (jobMetadata != null) {
                store.storeJobState(jobId, state);
            } else {
                logger.warn("Job not found in store: {}", jobId);
            }
        } catch (InvalidJobException e) {
            logger.error("Unexpected exception when handling job: {}", jobId, e);
        } catch (IOException e) {
            // no reasonable way to handle storage exceptions
            logger.error("Can't store job state: jobId={}, newState={}, error={}", jobId, state, e.getMessage(), e);
        } catch (InvalidJobStateChangeException e) {
            // ignore, must be already in terminal state
            logger.warn(jobId + ": error changing state to Failed: " + e.getMessage());
        } finally {
            jobMetrics.finish();
            excludedAgentsTracker.finish();
        }
        numJobTerminated.increment();
        statusSerializedObserver.onCompleted();
        resubmitRateLimiter.endJob(jobId);
        assignmentsPublisher.stop();
        if (enforceSlaFuture != null) {
            enforceSlaFuture.cancel(false);
        }
        if (jobMetadata != null) {
            auditLogService.submit(AuditLogEvent.of(AuditLogEvent.Type.JOB_TERMINATE, jobId, state + ": " + mesg, jobMetadata));
            eventBus.publish(new JobStateChangeEvent<>(jobId, JobState.Finished, System.currentTimeMillis(), jobMetadata));
        }
    }

    protected V2WorkerMetadata replaceWorker(V2WorkerMetadata replaced) throws InvalidJobStateChangeException, IOException, InvalidJobException {
        final V2JobMetadata job = getJobMetadata();
        V2StageMetadata stageMetadata = job.getStageMetadata(replaced.getStageNum());
        V2WorkerMetadata newWorker = store.replaceTerminatedWorker(
                new WorkerRequest(
                        stageMetadata.getMachineDefinition(), jobId,
                        replaced.getWorkerIndex(), workerNumberGenerator.getNextWorkerNumber(),
                        WorkerRequest.generateWorkerInstanceId(), cellInfoResolver.getCellName(),
                        job.getJarUrl(), 1, 1, stageMetadata.getNumWorkers(), namedJob.getName(),
                        stageMetadata.getMachineDefinition().getNumPorts(),
                        job.getParameters(),
                        job.getSla(), stageMetadata.getHardConstraints(), stageMetadata.getSoftConstraints(),
                        stageMetadata.getSecurityGroups(), stageMetadata.getAllocateIP(), jobDefinition.getSchedulingInfo(), false
                ),
                replaced
        );
        queueTask(newWorker);
        jobMetrics.updateTaskMetrics(newWorker);
        return newWorker;
    }

    @Override
    public void initialize(V2JobStore store) {
        this.store = store;
        V2JobMetadata job = store.getActiveJob(jobId);

        if (!V2JobState.isTerminalState(job.getState())) {
            workerNumberGenerator.init(store);
            final V2StageMetadata stageMetadata = job.getStageMetadata(1);
            final Collection<V2WorkerMetadata> taskByIndexMetadataSet =
                    stageMetadata == null ? null : stageMetadata.getWorkerByIndexMetadataSet();
            if (stageMetadata == null) {
                storeAndMarkJobTerminated(V2JobState.Failed, "Didn't find stage metadata");
                return;
            }
            final int numTasks = stageMetadata.getNumWorkers();
            Map<Integer, WorkerAssignments> stageAssignments = new HashMap<>();
            Map<Integer, WorkerHost> workerHosts = new HashMap<>();
            for (V2WorkerMetadata t : taskByIndexMetadataSet) {
                if (V2JobState.isRunningState(t.getState())) {
                    workerHosts.put(t.getWorkerNumber(), new WorkerHost(t.getSlave(), t.getWorkerIndex(), t.getPorts(),
                            t.getState(), t.getWorkerNumber()));
                    final StageSchedulingInfo schedulingInfo = jobDefinition.getSchedulingInfo().forStage(1);
                    WorkerRequest workerRequest = new WorkerRequest(schedulingInfo.getMachineDefinition(),
                            jobId, t.getWorkerIndex(), t.getWorkerNumber(), null,
                            cellInfoResolver.getCellName(), null, 1, 1,
                            schedulingInfo.getNumberOfInstances(),
                            jobDefinition.getName(), schedulingInfo.getMachineDefinition().getNumPorts(),
                            jobDefinition.getParameters(), jobDefinition.getJobSla(),
                            schedulingInfo.getHardConstraints(), schedulingInfo.getSoftConstraints(),
                            stageMetadata.getSecurityGroups(), stageMetadata.getAllocateIP(),
                            new SchedulingInfo(Collections.singletonMap(1, schedulingInfo)), false
                    );
                    if (t.getTwoLevelResources() != null) {
                        workerRequest.setTwoLevelResource(t.getTwoLevelResources());
                    }
                    schedulingService.initRunningTask(
                            createRequest(this, t, workerRequest),
                            t.getSlave()
                    );
                } else if (t.getState() == V2JobState.Accepted) {
                    queueTask(t);
                }
                if (!V2JobState.isTerminalState(t.getState())) {
                    jobMetrics.updateTaskMetrics(t);
                }
            }
            stageAssignments.put(1, new WorkerAssignments(1, numTasks, workerHosts));
            // update scheduling information to listeners
            assignmentsPublisher.init(store, stageAssignments);
            if (taskByIndexMetadataSet.size() < numTasks) {
                Set<Integer> existsIdx = taskByIndexMetadataSet.stream().map(V2WorkerMetadata::getWorkerIndex).collect(Collectors.toSet());
                int idx = 0;
                int currentSize = existsIdx.size();
                while (currentSize < numTasks) {
                    if (!existsIdx.contains(idx)) {
                        try {
                            int newWorkerNumber = workerNumberGenerator.getNextWorkerNumber();
                            currentSize++;
                            V2WorkerMetadata worker = store.storeNewWorker(new WorkerRequest(
                                    stageMetadata.getMachineDefinition(), jobId,
                                    idx, newWorkerNumber, WorkerRequest.generateWorkerInstanceId(),
                                    cellInfoResolver.getCellName(),
                                    job.getJarUrl(), 1, 1, numTasks, namedJob.getName(),
                                    stageMetadata.getMachineDefinition().getNumPorts(),
                                    job.getParameters(),
                                    job.getSla(), stageMetadata.getHardConstraints(), stageMetadata.getSoftConstraints(),
                                    stageMetadata.getSecurityGroups(), stageMetadata.getAllocateIP(),
                                    jobDefinition.getSchedulingInfo(), false
                            ));
                            queueTask(worker);
                            jobMetrics.updateTaskMetrics(worker);
                        } catch (IOException | InvalidJobException e) {
                            logger.error(jobId + ": Error storing new worker for index " + idx + " - " + e.getMessage());
                        }
                    }
                    idx++;
                }
            }
            final Collection<V2WorkerMetadata> allTasks = job.getStageMetadata(1).getWorkerByIndexMetadataSet();
            for (V2WorkerMetadata t : allTasks) {
                dequeueIfTerminalAndSendStatus(new Status(job.getJobId(), WorkerNaming.getTaskId(t), t.getStageNum(), t.getWorkerIndex(), t.getWorkerNumber(),
                        Status.TYPE.INFO, "" + t.getState(), t.getCompletionMessage(), t.getState()));
            }
        } else {
            assignmentsPublisher.stop();
        }
        if (!initialized.compareAndSet(false, true)) {
            throw new IllegalStateException("Job " + jobId + " already initialized");
        }
    }

    @Override
    public void initializeNewJob(final V2JobStore store)
            throws InvalidJobException {
        this.store = store;
        this.pendingInitialization = true;
        try {
            final V2JobMetadata job = store.storeNewJob(jobId, namedJob.getName(), jobDefinition);
            logger.info("Stored job " + jobId);
            workerNumberGenerator.init(store);
            assignmentsPublisher.init(store);
            final StageSchedulingInfo stageSchedulingInfo = jobDefinition.getSchedulingInfo().forStage(1);
            store.storeStage(new V2StageMetadataWritable(
                    jobId, 1, 1,
                    stageSchedulingInfo.getMachineDefinition(),
                    stageSchedulingInfo.getNumberOfInstances(),
                    stageSchedulingInfo.getHardConstraints(), stageSchedulingInfo.getSoftConstraints(),
                    stageSchedulingInfo.getSecurityGroups(), stageSchedulingInfo.getAllocateIP(),
                    stageSchedulingInfo.getScalingPolicy(), stageSchedulingInfo.getScalable(),
                    ServiceJobProcesses.newBuilder().build()
            ));
            eventBus.publish(new JobStateChangeEvent<>(jobId, JobState.Created, System.currentTimeMillis(), job));
            createAllWorkers();

            if (!initialized.compareAndSet(false, true)) {
                throw new IllegalStateException("Job " + jobId + " already initialized");
            }
        } catch (JobAlreadyExistsException | IOException | InvalidJobException e) {
            throw new InvalidJobException(e.getMessage(), e);
        }
    }

    @Override
    public void postInitializeNewJob() {
        pendingInitialization = false;
        for (ScheduledRequest request = taskQueueRequests.poll(); request != null; request = taskQueueRequests.poll()) {
            taskQueueAction.call(request);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    protected List<WorkerRequest> getInitialWorkers() {
        SchedulingInfo schedulingInfo = jobDefinition.getSchedulingInfo();
        StageSchedulingInfo stage = schedulingInfo.getStages().get(1);
        int numInstancesAtStage = stage.getNumberOfInstances();
        return getWorkersInRange(0, numInstancesAtStage, numInstancesAtStage);
    }

    protected List<WorkerRequest> getWorkersInRange(int beg, int en, int numInstancesAtStage) {
        List<WorkerRequest> workerRequests = new LinkedList<>();
        V2JobMetadata jobMetadata = getJobMetadata();
        SchedulingInfo schedulingInfo = jobDefinition.getSchedulingInfo();
        StageSchedulingInfo stage = schedulingInfo.getStages().get(1);
        int numPorts = stage.getMachineDefinition().getNumPorts();
        // add worker request for each instance required in stage
        // Possible optimization: create N < numInstancesAtStage initial workers and let others get created later
        for (int i = beg; i < en; i++) {
            int workerNumber = workerNumberGenerator.getNextWorkerNumber();
            String workerInstanceId = WorkerRequest.generateWorkerInstanceId();
            // during initialization worker number and index are identical
            WorkerRequest workerRequest = new WorkerRequest(stage.getMachineDefinition(),
                    jobMetadata.getJobId(), i, workerNumber, workerInstanceId,
                    cellInfoResolver.getCellName(), jobMetadata.getJarUrl(),
                    1, 1, numInstancesAtStage,
                    jobMetadata.getName(), numPorts,
                    jobDefinition.getParameters(),
                    jobDefinition.getJobSla(), stage.getHardConstraints(),
                    stage.getSoftConstraints(), stage.getSecurityGroups(), stage.getAllocateIP(),
                    schedulingInfo, false);
            workerRequests.add(workerRequest);
        }
        return workerRequests;
    }

    @Override
    public boolean isTaskValid(String taskId) {
        final V2JobMetadata jobMetadata = getJobMetadata();
        if (V2JobState.isTerminalState(jobMetadata.getState())) {
            return false;
        }
        try {
            final WorkerNaming.JobWorkerIdPair jobAndWorkerId = WorkerNaming.getJobAndWorkerId(taskId);
            return !V2JobState.isTerminalState(jobMetadata.getWorkerByNumber(jobAndWorkerId.workerNumber).getState());
        } catch (InvalidJobException e) {
            return false;
        }
    }

    @Override
    public void setMigrationDeadline(String taskId, long migrationDeadline) {
        final WorkerNaming.JobWorkerIdPair jobAndWorkerId = WorkerNaming.getJobAndWorkerId(taskId);
        V2JobMetadata jobMetadata = getJobMetadata();
        try {
            final V2WorkerMetadata task = jobMetadata.getWorkerByNumber(jobAndWorkerId.workerNumber);
            if (task.getMigrationDeadline() > 0) {
                return;
            }
        } catch (InvalidJobException e) {
            // ignore
        }
        try (AutoCloseable l = jobMetadata.obtainLock()) {
            store.storeWorkerMigrationDeadline(jobId, jobAndWorkerId.workerNumber, migrationDeadline);
        } catch (Exception e) {
            logger.warn(jobId + ": Unexpected error setting migration deadline on worker number " + jobAndWorkerId.workerNumber);
        }
    }
}
