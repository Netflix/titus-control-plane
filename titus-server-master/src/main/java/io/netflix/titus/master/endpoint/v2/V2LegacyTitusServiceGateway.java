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

package io.netflix.titus.master.endpoint.v2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import com.google.common.base.Preconditions;
import com.netflix.fenzo.queues.QueuableTask;
import com.netflix.fenzo.queues.TaskQueue;
import io.netflix.titus.api.endpoint.v2.rest.representation.AuditLog;
import io.netflix.titus.api.endpoint.v2.rest.representation.TaskInfo;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobInfo;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobState;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobType;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskInfo;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskState;
import io.netflix.titus.api.jobmanager.service.JobManagerException;
import io.netflix.titus.api.model.Page;
import io.netflix.titus.api.model.Pagination;
import io.netflix.titus.api.model.v2.JobSla;
import io.netflix.titus.api.model.v2.V2JobDefinition;
import io.netflix.titus.api.model.v2.V2JobDurationType;
import io.netflix.titus.api.model.v2.V2JobState;
import io.netflix.titus.api.model.v2.WorkerNaming;
import io.netflix.titus.api.model.v2.parameter.Parameter;
import io.netflix.titus.api.model.v2.parameter.Parameters;
import io.netflix.titus.api.service.TitusServiceException;
import io.netflix.titus.api.service.TitusServiceException.ErrorCode;
import io.netflix.titus.api.store.v2.InvalidJobException;
import io.netflix.titus.api.store.v2.V2JobMetadata;
import io.netflix.titus.api.store.v2.V2StageMetadata;
import io.netflix.titus.api.store.v2.V2WorkerMetadata;
import io.netflix.titus.common.util.DateTimeExt;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.ApiOperations;
import io.netflix.titus.master.config.CellInfoResolver;
import io.netflix.titus.master.config.MasterConfiguration;
import io.netflix.titus.master.endpoint.TitusServiceGateway;
import io.netflix.titus.master.endpoint.common.CellDecorator;
import io.netflix.titus.master.endpoint.common.ContextResolver;
import io.netflix.titus.master.endpoint.common.SchedulerUtil;
import io.netflix.titus.master.endpoint.common.TaskSummary;
import io.netflix.titus.master.endpoint.v2.rest.RestConfig;
import io.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;
import io.netflix.titus.master.job.JobMgrUtils;
import io.netflix.titus.master.job.V2JobOperations;
import io.netflix.titus.master.jobmanager.service.limiter.JobSubmitLimiter;
import io.netflix.titus.master.scheduler.SchedulingService;
import io.netflix.titus.master.service.management.ApplicationSlaManagementService;
import io.netflix.titus.master.store.NamedJobs;
import io.netflix.titus.runtime.endpoint.JobQueryCriteria;
import io.netflix.titus.runtime.endpoint.common.LogStorageInfo;
import io.netflix.titus.runtime.endpoint.common.LogStorageInfo.LogLinks;
import rx.Observable;

import static io.netflix.titus.master.endpoint.common.TitusServiceGatewayUtil.newObservable;

/**
 * {@link TitusServiceGateway} implementation that interacts with the legacy runtime layer, and supports data
 * conversion to v2 REST API.
 */
@Singleton
public class V2LegacyTitusServiceGateway extends V2EngineTitusServiceGateway<
        String, TitusJobSpec, TitusJobType, TitusJobInfo, TitusTaskInfo, TitusTaskState> {

    /**
     * Service jobs have single stage with id '1'.
     */
    private static final int SERVICE_STAGE = 1;

    private final MasterConfiguration config;
    private final RestConfig restConfig;
    private final V2JobOperations v2JobOperations;
    private final JobSubmitLimiter jobSubmitLimiter;
    private final ApplicationSlaManagementService applicationSlaManagementService;
    private final SchedulingService schedulingService;
    private final ContextResolver contextResolver;
    private final LogStorageInfo<V2WorkerMetadata> logStorageInfo;
    private final CellDecorator cellDecorator;

    @Inject
    public V2LegacyTitusServiceGateway(MasterConfiguration config,
                                       RestConfig restConfig,
                                       V2JobOperations v2JobOperations,
                                       JobSubmitLimiter jobSubmitLimiter,
                                       ApiOperations apiOperations,
                                       ApplicationSlaManagementService applicationSlaManagementService,
                                       SchedulingService schedulingService,
                                       ContextResolver contextResolver,
                                       CellInfoResolver cellInfoResolver,
                                       LogStorageInfo<V2WorkerMetadata> logStorageInfo
    ) {
        super(apiOperations);
        this.config = config;
        this.restConfig = restConfig;
        this.v2JobOperations = v2JobOperations;
        this.jobSubmitLimiter = jobSubmitLimiter;
        this.applicationSlaManagementService = applicationSlaManagementService;
        this.schedulingService = schedulingService;
        this.contextResolver = contextResolver;
        this.logStorageInfo = logStorageInfo;
        this.cellDecorator = new CellDecorator(cellInfoResolver::getCellName);
    }

    @Override
    public Observable<String> createJob(TitusJobSpec originalJobSpec) {
        return newObservable(subscriber -> {
            final TitusJobSpec jobSpec = cellDecorator.ensureCellInfo(originalJobSpec);
            final List<Parameter> parameters = TitusJobSpec.getParameters(jobSpec);

            JobSla jobSla = new JobSla(
                    jobSpec.getRetries(),
                    jobSpec.getRuntimeLimitSecs(),
                    0L,
                    JobSla.StreamSLAType.Lossy,
                    jobSpec.getType() == TitusJobType.service
                            ? V2JobDurationType.Perpetual
                            : V2JobDurationType.Transient,
                    null
            );
            V2JobDefinition jobDefinition = new V2JobDefinition(
                    NamedJobs.TitusJobName,
                    jobSpec.getUser(),
                    null,
                    jobSpec.getVersion(),
                    parameters,
                    jobSla,
                    0L,
                    jobSpec.getSchedulingInfo(),
                    0,
                    0,
                    null,
                    null
            );


            try {
                Optional<String> reserveStatus = jobSubmitLimiter.reserveId(jobDefinition);
                if (reserveStatus.isPresent()) {
                    subscriber.onError(TitusServiceException.newBuilder(ErrorCode.INVALID_ARGUMENT, reserveStatus.get()).build());
                    return;
                }
                Optional<String> limited = jobSubmitLimiter.checkIfAllowed(jobDefinition);

                if (limited.isPresent()) {
                    subscriber.onError(TitusServiceException.newBuilder(ErrorCode.INVALID_ARGUMENT, limited.get()).build());
                } else {
                    try {
                        subscriber.onNext(v2JobOperations.submit(jobDefinition));
                        subscriber.onCompleted();
                    } catch (IllegalArgumentException e) {
                        subscriber.onError(TitusServiceException.invalidArgument(e));
                    }
                }
            } finally {
                jobSubmitLimiter.releaseId(jobDefinition);
            }
        });
    }

    @Override
    public Observable<Void> killJob(String user, String jobId) {
        return newObservable(subscriber -> {
            if (apiOperations.killJob(jobId, user)) {
                subscriber.onCompleted();
            } else {
                subscriber.onError(TitusServiceException.jobNotFound(jobId));
            }
        });
    }

    @Override
    public Observable<TitusJobInfo> findJobById(String jobId, boolean includeArchivedTasks, Set<TitusTaskState> taskStates) {
        return newObservable(subscriber -> {
            V2JobMetadata jobMetadata = apiOperations.getJobMetadata(jobId);
            if (jobMetadata == null) {
                subscriber.onError(TitusServiceException.jobNotFound(jobId));
                return;
            }

            TitusJobInfo titusJobInfo = buildTitusJobInfo(jobMetadata, includeArchivedTasks, taskStates);
            subscriber.onNext(titusJobInfo);
            subscriber.onCompleted();
        });
    }

    @Override
    public Pair<List<TitusJobInfo>, Pagination> findJobsByCriteria(JobQueryCriteria<TitusTaskState, TitusJobType> queryCriteria, Optional<Page> page) {
        Preconditions.checkArgument(!page.isPresent(), "V2 API does not support pagination");

        // Do not pass limit, as we will do more filtering here
        boolean includeArchived = !restConfig.isArchiveDataQueryRestricted() && queryCriteria.isIncludeArchived();
        final List<V2JobMetadata> allJobs = apiOperations.getAllJobsMetadata(!includeArchived, -1);
        int limit = queryCriteria.getLimit() < 1 ? Integer.MAX_VALUE : queryCriteria.getLimit();
        if (allJobs != null) {
            int count = 0;
            List<TitusJobInfo> v2Jobs = new ArrayList<>();
            for (V2JobMetadata job : allJobs) {
                if (JobQueryCriteriaEvaluator.matches(job, queryCriteria)) {
                    TitusJobInfo jobInfo = buildTitusJobInfo(job, includeArchived, queryCriteria.getTaskStates());
                    if (jobInfo.getInstancesDesired() == 0 || !jobInfo.getTasks().isEmpty()) {
                        v2Jobs.add(jobInfo);
                        count++;
                        if (count >= limit) {
                            break;
                        }
                    }
                }
            }
            return Pair.of(v2Jobs, null);
        }
        return Pair.of(Collections.emptyList(), null);
    }

    @Override
    public Pair<List<TitusTaskInfo>, Pagination> findTasksByCriteria(JobQueryCriteria<TitusTaskState, TitusJobType> queryCriteria, Optional<Page> page) {
        throw TitusServiceException.notSupported();
    }

    @Override
    public Observable<TitusTaskInfo> findTaskById(String taskId) {
        return newObservable(subscriber -> {

            Pair<V2JobMetadata, V2WorkerMetadata> jobWorkerPair;
            try {
                jobWorkerPair = findWorker(taskId);
            } catch (TitusServiceException e) {
                subscriber.onError(e);
                return;
            }
            V2JobMetadata jobMetadata = jobWorkerPair.getLeft();
            V2WorkerMetadata mwmd = jobWorkerPair.getRight();

            final TitusJobSpec jobSpec = TitusJobSpec.getSpec(jobMetadata);
            Map<Integer, Integer> portMapping = new HashMap<>();
            final int[] portsRequested = jobSpec.getPorts();
            if (V2JobState.isRunningState(mwmd.getState()) && portsRequested != null && portsRequested.length > 0) {
                final List<Integer> portsAssigned = mwmd.getPorts();
                int idx = 0;
                for (int p : portsRequested) {
                    if (idx < portsAssigned.size()) {
                        portMapping.put(p, portsAssigned.get(idx));
                    }
                    idx++;
                }
            }
            TitusTaskState titusState = TitusTaskState.getTitusState(mwmd.getState(), mwmd.getReason());
            String slaveHost = mwmd.getSlave();
            final String hostInstanceId = mwmd.getSlaveAttributes().get("id");
            final String itype = mwmd.getSlaveAttributes().get("itype");
            LogLinks logLinks = logStorageInfo.getLinks(mwmd);

            TitusTaskInfo taskInfo = new TitusTaskInfo(
                    WorkerNaming.getWorkerName(jobMetadata.getJobId(), mwmd.getWorkerIndex(), mwmd.getWorkerNumber()),
                    mwmd.getWorkerInstanceId(),
                    jobMetadata.getJobId(),
                    mwmd.getCell(),
                    titusState,
                    jobSpec.getApplicationName(), jobSpec.getCpu(), jobSpec.getMemory(), jobSpec.getNetworkMbps(), jobSpec.getDisk(),
                    portMapping, jobSpec.getGpu(), jobSpec.getEnv(), jobSpec.getVersion(), jobSpec.getEntryPoint(), slaveHost,
                    DateTimeExt.toUtcDateTimeString(mwmd.getAcceptedAt()),
                    DateTimeExt.toUtcDateTimeString(mwmd.getLaunchedAt()),
                    DateTimeExt.toUtcDateTimeString(mwmd.getStartedAt()),
                    DateTimeExt.toUtcDateTimeString(mwmd.getCompletedAt()),
                    DateTimeExt.toUtcDateTimeString(mwmd.getMigrationDeadline()),
                    mwmd.getCompletionMessage(),
                    mwmd.getStatusData(),
                    logLinks.getLiveLink().orElse(null),
                    logLinks.getLogLink().orElse(null),
                    logLinks.getSnapshotLink().orElse(null),
                    hostInstanceId,
                    itype
            );

            subscriber.onNext(taskInfo);
            subscriber.onCompleted();
        });
    }

    @Override
    public Observable<Void> resizeJob(String user, String jobId, int desired, int min, int max) {
        return newObservable(subscriber -> {
            try {
                apiOperations.updateInstanceCounts(jobId, SERVICE_STAGE, min, desired, max, user);
                subscriber.onCompleted();
            } catch (JobManagerException e) {
                subscriber.onError(TitusServiceException.newBuilder(ErrorCode.JOB_UPDATE_NOT_ALLOWED, e.getMessage()).withCause(e).build());
            } catch (InvalidJobException e) {
                subscriber.onError(TitusServiceException.jobNotFound(jobId, e));
            }
        });
    }

    @Override
    public Observable<Void> updateJobProcesses(String user, String jobId, boolean disableDecreaseDesired, boolean disableIncreaseDesired) {
        return newObservable(subscriber -> {
            try {
                apiOperations.updateJobProcesses(jobId, SERVICE_STAGE, disableIncreaseDesired, disableDecreaseDesired, user);
                subscriber.onCompleted();
            } catch (InvalidJobException e) {
                subscriber.onError(TitusServiceException.jobNotFound(jobId, e));
            }
        });
    }

    @Override
    public Observable<Void> changeJobInServiceStatus(String user, String serviceJobId, boolean inService) {
        return newObservable(subscriber -> {
            try {
                apiOperations.updateInServiceStatus(serviceJobId, SERVICE_STAGE, inService, user);
                subscriber.onCompleted();
            } catch (InvalidJobException e) {
                subscriber.onError(TitusServiceException.jobNotFound(serviceJobId, e));
            } catch (UnsupportedOperationException e) {
                subscriber.onError(TitusServiceException.newBuilder(ErrorCode.UNSUPPORTED_JOB_TYPE, "invalid job type").withCause(e).build());
            }
        });
    }

    @Override
    public Observable<Void> observeJobs() {
        return Observable.error(TitusServiceException.notSupported());
    }

    @Override
    public Observable<Void> observeJob(String jobId) {
        return Observable.error(TitusServiceException.notSupported());
    }

    @Override
    public Observable<List<TaskSummary>> getTaskSummary() {
        return newObservable(subscriber -> {
            final Map<TaskQueue.TaskState, Collection<QueuableTask>> tasksMap =
                    SchedulerUtil.blockAndGetTasksFromQueue(schedulingService);
            if (tasksMap == null) {
                throw new WebApplicationException(new TimeoutException("Timed out waiting for queue list"), Response.Status.INTERNAL_SERVER_ERROR);
            }

            Map<String, Map<TaskQueue.TaskState, Integer>> result = new HashMap<>();
            for (TaskQueue.TaskState s : TaskQueue.TaskState.values()) {
                if (tasksMap.get(s) != null) {
                    for (QueuableTask t : tasksMap.get(s)) {
                        final V2JobMetadata jobMetadata = getJobMetadata(t.getId());
                        if (jobMetadata != null) {
                            String appName = Parameters.getAppName(jobMetadata.getParameters());
                            if (appName == null) {
                                appName = "NULL";
                            }
                            result.putIfAbsent(appName, new HashMap<>());
                            final Map<TaskQueue.TaskState, Integer> stateIntegerMap = result.get(appName);
                            stateIntegerMap.putIfAbsent(s, 0);
                            stateIntegerMap.put(s, stateIntegerMap.get(s) + 1);
                        }
                    }
                }
            }
            List<TaskSummary> summaries = new LinkedList<>();
            for (Map.Entry<String, Map<TaskQueue.TaskState, Integer>> entry : result.entrySet()) {
                summaries.add(new TaskSummary(entry.getKey(), entry.getValue()));
            }
            subscriber.onNext(summaries);
            subscriber.onCompleted();
        });
    }

    private V2JobMetadata getJobMetadata(String taskId) {
        final WorkerNaming.JobWorkerIdPair jobAndWorkerId = WorkerNaming.getJobAndWorkerId(taskId);
        return apiOperations.getJobMetadata(jobAndWorkerId.jobId);
    }

    private TitusJobInfo buildTitusJobInfo(V2JobMetadata jobMetadata, boolean includeArchivedTasks, Set<TitusTaskState> taskStates) {
        List<TaskInfo> tasks = new ArrayList<>();
        V2StageMetadata stageMetadata = jobMetadata.getStageMetadata(1);
        List<V2WorkerMetadata> allWorkers = new ArrayList<>(stageMetadata.getAllWorkers());

        if (includeArchivedTasks) {
            final Set<String> currentWorkerInstanceIds = allWorkers.stream().map(V2WorkerMetadata::getWorkerInstanceId).collect(Collectors.toCollection(HashSet::new));
            final List<? extends V2WorkerMetadata> archivedWorkers = apiOperations.getArchivedWorkers(jobMetadata.getJobId());
            final List<? extends V2WorkerMetadata> archivedWorkersOnly = archivedWorkers.stream().filter(w -> !currentWorkerInstanceIds.contains(w.getWorkerInstanceId())).collect(Collectors.toList());
            allWorkers.addAll(archivedWorkersOnly);
        }

        for (V2WorkerMetadata mwmd : allWorkers) {
            TitusTaskState state = TitusTaskState.getTitusState(mwmd.getState(), mwmd.getReason());
            if (!JobMgrUtils.isTombStoned(mwmd) && (taskStates.isEmpty() || taskStates.contains(state))) {
                String id = WorkerNaming.getWorkerName(mwmd.getJobId(), mwmd.getWorkerIndex(), mwmd.getWorkerNumber());
                String host = mwmd.getSlave();
                String hostInstanceId = mwmd.getSlaveAttributes().get("id");
                String itype = mwmd.getSlaveAttributes().get("itype");
                LogLinks logLinks = logStorageInfo.getLinks(mwmd);

                tasks.add(new TaskInfo(
                        id,
                        mwmd.getWorkerInstanceId(),
                        mwmd.getCell(),
                        state,
                        host,
                        hostInstanceId,
                        itype,
                        config.getRegion(),
                        getWorkerZone(mwmd),
                        DateTimeExt.toUtcDateTimeString(mwmd.getAcceptedAt()),
                        DateTimeExt.toUtcDateTimeString(mwmd.getLaunchedAt()),
                        DateTimeExt.toUtcDateTimeString(mwmd.getStartedAt()),
                        DateTimeExt.toUtcDateTimeString(mwmd.getCompletedAt()),
                        DateTimeExt.toUtcDateTimeString(mwmd.getMigrationDeadline()),
                        mwmd.getCompletionMessage(), mwmd.getStatusData(),
                        logLinks.getLiveLink().orElse(null),
                        logLinks.getLogLink().orElse(null),
                        logLinks.getSnapshotLink().orElse(null)
                ));
            }
        }

        final List<AuditLog> auditLogs = AuditLog.fromV2AuditLogEvent(jobMetadata.getLatestAuditLogEvents());
        TitusJobSpec jobSpec = TitusJobSpec.getSpec(jobMetadata);

        return new TitusJobInfo(jobMetadata.getJobId(), jobSpec.getName(), jobSpec.getType(), jobSpec.getLabels(),
                jobSpec.getApplicationName(), jobSpec.getAppName(), jobSpec.getUser(), jobSpec.getVersion(),
                jobSpec.getEntryPoint(), jobSpec.isInService(), TitusJobState.getTitusState(jobMetadata.getState()),
                jobSpec.getInstances(), jobSpec.getInstancesMin(),
                jobSpec.getInstancesMax(), jobSpec.getInstancesDesired(), jobSpec.getCpu(), jobSpec.getMemory(),
                jobSpec.getNetworkMbps(), jobSpec.getDisk(), jobSpec.getPorts(), jobSpec.getGpu(),
                jobSpec.getJobGroupStack(), jobSpec.getJobGroupDetail(), jobSpec.getJobGroupSequence(),
                jobSpec.getCapacityGroup(), jobSpec.getMigrationPolicy(), jobSpec.getEnv(),
                contextResolver.resolveContext(jobMetadata.getJobId()), jobSpec.getRetries(), jobSpec.getRuntimeLimitSecs(), jobSpec.isAllocateIpAddress(),
                jobSpec.getIamProfile(), jobSpec.getSecurityGroups(), jobSpec.getEfs(), jobSpec.getEfsMounts(),
                DateTimeExt.toUtcDateTimeString(jobMetadata.getSubmittedAt()),
                jobSpec.getSoftConstraints(), jobSpec.getHardConstraints(), tasks, auditLogs, stageMetadata.getJobProcesses());
    }

    private String getWorkerZone(V2WorkerMetadata mwmd) {
        if (mwmd.getSlave() == null) {
            return null;
        }
        final Map<String, String> slaveAttributes = mwmd.getSlaveAttributes();
        if (!slaveAttributes.isEmpty()) {
            String val = slaveAttributes.get(config.getHostZoneAttributeName());
            if (val != null) {
                return val;
            }
        }
        return "Unknown";
    }
}
