/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.titus.master.jobmanager.endpoint.v3.grpc;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.google.protobuf.Empty;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobProcesses;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudget;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudgetFunctions;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.model.Pagination;
import com.netflix.titus.api.model.PaginationUtil;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.common.model.validator.ValidationError;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.ProtobufCopy;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobCapacityUpdate;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobDisruptionBudget;
import com.netflix.titus.grpc.protogen.JobDisruptionBudgetUpdate;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobProcessesUpdate;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.JobStatusUpdate;
import com.netflix.titus.grpc.protogen.ObserveJobsQuery;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskId;
import com.netflix.titus.grpc.protogen.TaskKillRequest;
import com.netflix.titus.grpc.protogen.TaskMoveRequest;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import com.netflix.titus.grpc.protogen.TaskStatus;
import com.netflix.titus.master.config.CellInfoResolver;
import com.netflix.titus.master.endpoint.common.CellDecorator;
import com.netflix.titus.master.endpoint.grpc.GrpcEndpointConfiguration;
import com.netflix.titus.master.jobmanager.service.JobManagerUtil;
import com.netflix.titus.master.model.ResourceDimensions;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import com.netflix.titus.runtime.endpoint.JobQueryCriteria;
import com.netflix.titus.runtime.endpoint.common.LogStorageInfo;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataUtils;
import com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
import com.netflix.titus.runtime.endpoint.v3.grpc.query.V3JobQueryCriteriaEvaluator;
import com.netflix.titus.runtime.endpoint.v3.grpc.query.V3TaskQueryCriteriaEvaluator;
import com.netflix.titus.runtime.jobmanager.JobManagerCursors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;

import static com.netflix.titus.api.FeatureFlagModule.DISRUPTION_BUDGET_FEATURE;
import static com.netflix.titus.api.jobmanager.model.job.sanitizer.JobSanitizerBuilder.JOB_STRICT_SANITIZER;
import static com.netflix.titus.runtime.connector.jobmanager.JobManagementClient.JOB_MINIMUM_FIELD_SET;
import static com.netflix.titus.runtime.connector.jobmanager.JobManagementClient.TASK_MINIMUM_FIELD_SET;
import static com.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toGrpcPagination;
import static com.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toJobQueryCriteria;
import static com.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toPage;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.safeOnError;
import static com.netflix.titus.runtime.endpoint.metadata.CallMetadataUtils.execute;
import static com.netflix.titus.runtime.endpoint.v3.grpc.TitusPaginationUtils.checkPageIsValid;

@Singleton
public class DefaultJobManagementServiceGrpc extends JobManagementServiceGrpc.JobManagementServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(DefaultJobManagementServiceGrpc.class);

    private static final JobChangeNotification SNAPSHOT_END_MARKER = JobChangeNotification.newBuilder()
            .setSnapshotEnd(JobChangeNotification.SnapshotEnd.newBuilder())
            .build();

    private final GrpcEndpointConfiguration configuration;
    private final AgentManagementService agentManagementService;
    private final ApplicationSlaManagementService capacityGroupService;
    private final V3JobOperations jobOperations;
    private final LogStorageInfo<com.netflix.titus.api.jobmanager.model.job.Task> logStorageInfo;
    private final EntitySanitizer entitySanitizer;
    private final Predicate<com.netflix.titus.api.jobmanager.model.job.JobDescriptor> disruptionBudgetEnabledPredicate;
    private final CallMetadataResolver callMetadataResolver;
    private final CellDecorator cellDecorator;
    private final TitusRuntime titusRuntime;

    @Inject
    public DefaultJobManagementServiceGrpc(GrpcEndpointConfiguration configuration,
                                           AgentManagementService agentManagementService,
                                           ApplicationSlaManagementService capacityGroupService,
                                           V3JobOperations jobOperations,
                                           LogStorageInfo<com.netflix.titus.api.jobmanager.model.job.Task> logStorageInfo,
                                           @Named(JOB_STRICT_SANITIZER) EntitySanitizer entitySanitizer,
                                           @Named(DISRUPTION_BUDGET_FEATURE) Predicate<com.netflix.titus.api.jobmanager.model.job.JobDescriptor> disruptionBudgetEnabledPredicate,
                                           CallMetadataResolver callMetadataResolver,
                                           CellInfoResolver cellInfoResolver,
                                           TitusRuntime titusRuntime) {
        this.configuration = configuration;
        this.agentManagementService = agentManagementService;
        this.capacityGroupService = capacityGroupService;
        this.jobOperations = jobOperations;
        this.logStorageInfo = logStorageInfo;
        this.entitySanitizer = entitySanitizer;
        this.disruptionBudgetEnabledPredicate = disruptionBudgetEnabledPredicate;
        this.callMetadataResolver = callMetadataResolver;
        this.cellDecorator = new CellDecorator(cellInfoResolver::getCellName);
        this.titusRuntime = titusRuntime;
    }

    @Override
    public void createJob(JobDescriptor jobDescriptor, StreamObserver<JobId> responseObserver) {
        execute(callMetadataResolver, responseObserver, callMetadata ->
                validateAndConvertJobDescriptorToCoreModel(jobDescriptor, responseObserver).ifPresent(sanitized ->
                        jobOperations.createJob(sanitized).subscribe(
                                jobId -> responseObserver.onNext(JobId.newBuilder().setId(jobId).build()),
                                e -> safeOnError(logger, e, responseObserver),
                                responseObserver::onCompleted
                        )));
    }

    private Optional<com.netflix.titus.api.jobmanager.model.job.JobDescriptor> validateAndConvertJobDescriptorToCoreModel(JobDescriptor jobDescriptor, StreamObserver<JobId> responseObserver) {
        if (configuration.isJobSizeValidationEnabled()) {
            com.netflix.titus.api.jobmanager.model.job.JobDescriptor coreJobDescriptor = V3GrpcModelConverters.toCoreJobDescriptor(jobDescriptor);

            Tier tier = findTier(coreJobDescriptor);
            ResourceDimension requestedResources = toResourceDimension(coreJobDescriptor.getContainer().getContainerResources());
            List<ResourceDimension> tierResourceLimits = getTierResourceLimits(tier);
            if (isTooLarge(requestedResources, tierResourceLimits)) {
                safeOnError(logger,
                        JobManagerException.invalidContainerResources(tier, requestedResources, tierResourceLimits),
                        responseObserver
                );
                return Optional.empty();
            }
        }

        com.netflix.titus.api.jobmanager.model.job.JobDescriptor coreJobDescriptor;
        try {
            coreJobDescriptor = V3GrpcModelConverters.toCoreJobDescriptor(cellDecorator.ensureCellInfo(jobDescriptor));
        } catch (Exception e) {
            safeOnError(logger, TitusServiceException.invalidArgument(e), responseObserver);
            return Optional.empty();
        }

        com.netflix.titus.api.jobmanager.model.job.JobDescriptor sanitizedCoreJobDescriptor = entitySanitizer.sanitize(coreJobDescriptor).orElse(coreJobDescriptor);

        Set<ValidationError> violations = entitySanitizer.validate(sanitizedCoreJobDescriptor);
        if (!violations.isEmpty()) {
            safeOnError(logger, TitusServiceException.invalidArgument(violations), responseObserver);
            return Optional.empty();
        }

        if (!disruptionBudgetEnabledPredicate.test(sanitizedCoreJobDescriptor)) {
            if (!DisruptionBudgetFunctions.isLegacyJobDescriptor(sanitizedCoreJobDescriptor)) {
                safeOnError(logger, TitusServiceException.invalidArgument("Disruption budget not enabled for this application"), responseObserver);
                return Optional.empty();
            }
        }

        return Optional.of(sanitizedCoreJobDescriptor);
    }

    @Override
    public void findJobs(JobQuery jobQuery, StreamObserver<JobQueryResult> responseObserver) {
        if (!checkPageIsValid(jobQuery.getPage(), responseObserver)) {
            return;
        }

        try {
            // We need to find all jobs to get the total number of them.
            List<com.netflix.titus.api.jobmanager.model.job.Job<?>> allFilteredJobs = jobOperations.findJobs(
                    new V3JobQueryCriteriaEvaluator(toJobQueryCriteria(jobQuery), titusRuntime),
                    0,
                    Integer.MAX_VALUE / 2
            );

            Pair<List<com.netflix.titus.api.jobmanager.model.job.Job<?>>, Pagination> queryResult = PaginationUtil.takePageWithCursor(
                    toPage(jobQuery.getPage()),
                    allFilteredJobs,
                    JobManagerCursors.coreJobCursorOrderComparator(),
                    JobManagerCursors::coreJobIndexOf,
                    JobManagerCursors::newCoreCursorFrom
            );
            List<Job> grpcJobs = queryResult.getLeft().stream().map(V3GrpcModelConverters::toGrpcJob).collect(Collectors.toList());

            JobQueryResult grpcQueryResult;
            if (jobQuery.getFieldsList().isEmpty()) {
                grpcQueryResult = toJobQueryResult(grpcJobs, queryResult.getRight());
            } else {
                Set<String> fields = new HashSet<>(jobQuery.getFieldsList());
                fields.addAll(JOB_MINIMUM_FIELD_SET);
                grpcQueryResult = toJobQueryResult(grpcJobs.stream().map(j -> ProtobufCopy.copy(j, fields)).collect(Collectors.toList()), queryResult.getRight());
            }

            responseObserver.onNext(grpcQueryResult);
            responseObserver.onCompleted();
        } catch (Exception e) {
            safeOnError(logger, e, responseObserver);
        }
    }

    @Override
    public void findJob(JobId request, StreamObserver<Job> responseObserver) {
        String id = request.getId();

        try {
            jobOperations.getJob(id)
                    .map(j -> Observable.just(V3GrpcModelConverters.toGrpcJob(j)))
                    .orElseGet(() -> Observable.error(JobManagerException.jobNotFound(id)))
                    .subscribe(
                            responseObserver::onNext,
                            e -> safeOnError(logger, e, responseObserver),
                            responseObserver::onCompleted
                    );
        } catch (Exception e) {
            safeOnError(logger, e, responseObserver);
        }
    }

    @Override
    public void findTasks(TaskQuery taskQuery, StreamObserver<TaskQueryResult> responseObserver) {
        if (!checkPageIsValid(taskQuery.getPage(), responseObserver)) {
            return;
        }

        try {
            // We need to find all tasks to get the total number of them.
            List<com.netflix.titus.api.jobmanager.model.job.Task> allFilteredTasks = jobOperations.findTasks(
                    new V3TaskQueryCriteriaEvaluator(toJobQueryCriteria(taskQuery), titusRuntime),
                    0,
                    Integer.MAX_VALUE / 2
            ).stream().map(Pair::getRight).collect(Collectors.toList());

            Pair<List<com.netflix.titus.api.jobmanager.model.job.Task>, Pagination> queryResult = PaginationUtil.takePageWithCursor(
                    toPage(taskQuery.getPage()),
                    allFilteredTasks,
                    JobManagerCursors.coreTaskCursorOrderComparator(),
                    JobManagerCursors::coreTaskIndexOf,
                    JobManagerCursors::newCoreCursorFrom
            );

            List<Task> grpcTasks = queryResult.getLeft().stream().map(t -> V3GrpcModelConverters.toGrpcTask(t, logStorageInfo)).collect(Collectors.toList());

            TaskQueryResult grpcQueryResult;
            if (taskQuery.getFieldsList().isEmpty()) {
                grpcQueryResult = toTaskQueryResult(grpcTasks, queryResult.getRight());
            } else {
                Set<String> fields = new HashSet<>(taskQuery.getFieldsList());
                fields.addAll(TASK_MINIMUM_FIELD_SET);
                grpcQueryResult = toTaskQueryResult(grpcTasks.stream().map(t -> ProtobufCopy.copy(t, fields)).collect(Collectors.toList()), queryResult.getRight());
            }

            responseObserver.onNext(grpcQueryResult);
            responseObserver.onCompleted();
        } catch (Exception e) {
            safeOnError(logger, e, responseObserver);
        }
    }

    @Override
    public void findTask(TaskId request, StreamObserver<Task> responseObserver) {
        String id = request.getId();

        try {
            jobOperations.findTaskById(id)
                    .map(p -> {
                        com.netflix.titus.api.jobmanager.model.job.Task task = p.getRight();
                        return Observable.just(V3GrpcModelConverters.toGrpcTask(task, logStorageInfo));
                    })
                    .orElseGet(() -> Observable.error(JobManagerException.taskNotFound(id)))
                    .subscribe(
                            responseObserver::onNext,
                            e -> safeOnError(logger, e, responseObserver),
                            responseObserver::onCompleted
                    );
        } catch (Exception e) {
            safeOnError(logger, e, responseObserver);
        }
    }

    @Override
    public void updateJobCapacity(JobCapacityUpdate request, StreamObserver<Empty> responseObserver) {
        execute(callMetadataResolver, responseObserver, callMetadata -> {
            com.netflix.titus.api.jobmanager.model.job.Capacity newCapacity = V3GrpcModelConverters.toCoreCapacity(request.getCapacity());

            Set<ValidationError> violations = entitySanitizer.validate(newCapacity);
            if (!violations.isEmpty()) {
                safeOnError(logger, TitusServiceException.invalidArgument(violations), responseObserver);
                return;
            }

            jobOperations.updateJobCapacity(request.getJobId(), newCapacity).subscribe(
                    nothing -> {
                    },
                    e -> safeOnError(logger, e, responseObserver),
                    () -> {
                        responseObserver.onNext(Empty.getDefaultInstance());
                        responseObserver.onCompleted();
                    }
            );
        });
    }

    @Override
    public void updateJobProcesses(JobProcessesUpdate request, StreamObserver<Empty> responseObserver) {
        execute(callMetadataResolver, responseObserver, callMetadata -> {
            ServiceJobProcesses serviceJobProcesses = V3GrpcModelConverters.toCoreServiceJobProcesses(request.getServiceJobProcesses());

            jobOperations.updateServiceJobProcesses(request.getJobId(), serviceJobProcesses).subscribe(
                    nothing -> {
                    },
                    e -> safeOnError(logger, e, responseObserver),
                    () -> {
                        responseObserver.onNext(Empty.getDefaultInstance());
                        responseObserver.onCompleted();
                    }
            );
        });
    }

    @Override
    public void updateJobStatus(JobStatusUpdate request, StreamObserver<Empty> responseObserver) {
        execute(callMetadataResolver, responseObserver, callMetadata ->
                jobOperations.updateJobStatus(request.getId(), request.getEnableStatus()).subscribe(
                        nothing -> {
                        },
                        e -> safeOnError(logger, e, responseObserver),
                        () -> {
                            responseObserver.onNext(Empty.getDefaultInstance());
                            responseObserver.onCompleted();
                        }
                ));
    }

    @Override
    public void updateJobDisruptionBudget(JobDisruptionBudgetUpdate request, StreamObserver<Empty> responseObserver) {
        execute(callMetadataResolver, responseObserver, callMetadata -> {
            com.netflix.titus.api.jobmanager.model.job.Job<?> job = jobOperations.getJob(request.getJobId()).orElse(null);
            if (job == null) {
                responseObserver.onError(JobManagerException.jobNotFound(request.getJobId()));
                return;
            }
            validateAndConvertJobDisruptionBudgetToCoreModel(job, request.getDisruptionBudget(), responseObserver).ifPresent(sanitized ->
                    jobOperations.updateJobDisruptionBudget(request.getJobId(), sanitized).subscribe(
                            nothing -> {
                            },
                            e -> safeOnError(logger, e, responseObserver),
                            () -> {
                                responseObserver.onNext(Empty.getDefaultInstance());
                                responseObserver.onCompleted();
                            }
                    )
            );
        });
    }

    private Optional<DisruptionBudget> validateAndConvertJobDisruptionBudgetToCoreModel(com.netflix.titus.api.jobmanager.model.job.Job<?> coreJob,
                                                                                        JobDisruptionBudget grpcDisruptionBudget,
                                                                                        StreamObserver<Empty> responseObserver) {
        if (!disruptionBudgetEnabledPredicate.test(coreJob.getJobDescriptor())) {
            safeOnError(logger, TitusServiceException.invalidArgument("Disruption budget not enabled for this application"), responseObserver);
            return Optional.empty();
        }

        DisruptionBudget coreDisruptionBudget;
        try {
            coreDisruptionBudget = V3GrpcModelConverters.toCoreDisruptionBudget(grpcDisruptionBudget);
        } catch (Exception e) {
            safeOnError(logger, TitusServiceException.invalidArgument(e), responseObserver);
            return Optional.empty();
        }

        DisruptionBudget sanitizedCoreDisruptionBudget = entitySanitizer.sanitize(coreDisruptionBudget).orElse(coreDisruptionBudget);

        Set<ValidationError> violations = entitySanitizer.validate(sanitizedCoreDisruptionBudget);
        if (!violations.isEmpty()) {
            safeOnError(logger, TitusServiceException.invalidArgument(violations), responseObserver);
            return Optional.empty();
        }

        return Optional.of(sanitizedCoreDisruptionBudget);
    }


    @Override
    public void killJob(JobId request, StreamObserver<Empty> responseObserver) {
        execute(callMetadataResolver, responseObserver, callMetadata ->
                jobOperations.killJob(request.getId()).subscribe(
                        nothing -> {
                        },
                        e -> safeOnError(logger, e, responseObserver),
                        () -> {
                            responseObserver.onNext(Empty.getDefaultInstance());
                            responseObserver.onCompleted();
                        }
                ));
    }

    @Override
    public void killTask(TaskKillRequest request, StreamObserver<Empty> responseObserver) {
        execute(callMetadataResolver, responseObserver, callMetadata -> {
            String reason = String.format("User initiated task kill: %s", CallMetadataUtils.toReasonString(callMetadata));
            jobOperations.killTask(request.getTaskId(), request.getShrink(), reason).subscribe(
                    nothing -> {
                    },
                    e -> safeOnError(logger, e, responseObserver),
                    () -> {
                        responseObserver.onNext(Empty.getDefaultInstance());
                        responseObserver.onCompleted();
                    }
            );
        });
    }

    @Override
    public void moveTask(TaskMoveRequest request, StreamObserver<Empty> responseObserver) {
        execute(callMetadataResolver, responseObserver, callMetadata -> {
            jobOperations.moveServiceTask(request.getSourceJobId(), request.getTargetJobId(), request.getTaskId()).subscribe(
                    nothing -> {
                    },
                    e -> safeOnError(logger, e, responseObserver),
                    () -> {
                        responseObserver.onNext(Empty.getDefaultInstance());
                        responseObserver.onCompleted();
                    }
            );
        });
    }

    @Override
    public void observeJobs(ObserveJobsQuery query, StreamObserver<JobChangeNotification> responseObserver) {
        JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> criteria = toJobQueryCriteria(query);
        V3JobQueryCriteriaEvaluator jobsPredicate = new V3JobQueryCriteriaEvaluator(criteria, titusRuntime);
        V3TaskQueryCriteriaEvaluator tasksPredicate = new V3TaskQueryCriteriaEvaluator(criteria, titusRuntime);

        Observable<JobChangeNotification> eventStream = jobOperations.observeJobs(jobsPredicate, tasksPredicate)
                .map(event -> V3GrpcModelConverters.toGrpcJobChangeNotification(event, logStorageInfo))
                .compose(ObservableExt.head(() -> {
                    List<JobChangeNotification> snapshot = createJobsSnapshot(jobsPredicate, tasksPredicate);
                    snapshot.add(SNAPSHOT_END_MARKER);
                    return snapshot;
                }))
                .doOnError(e -> logger.error("Unexpected error in jobs event stream", e));

        Subscription subscription = eventStream.subscribe(
                responseObserver::onNext,
                e -> responseObserver.onError(
                        new StatusRuntimeException(Status.INTERNAL
                                .withDescription("All jobs monitoring stream terminated with an error")
                                .withCause(e))
                ),
                responseObserver::onCompleted
        );

        ServerCallStreamObserver<JobChangeNotification> serverObserver = (ServerCallStreamObserver<JobChangeNotification>) responseObserver;
        serverObserver.setOnCancelHandler(subscription::unsubscribe);
    }

    @Override
    public void observeJob(JobId request, StreamObserver<JobChangeNotification> responseObserver) {
        String jobId = request.getId();
        Observable<JobChangeNotification> eventStream = jobOperations.observeJob(jobId)
                .map(event -> V3GrpcModelConverters.toGrpcJobChangeNotification(event, logStorageInfo))
                .compose(ObservableExt.head(() -> {
                    List<JobChangeNotification> snapshot = createJobSnapshot(jobId);
                    snapshot.add(SNAPSHOT_END_MARKER);
                    return snapshot;
                }))
                .doOnError(e -> {
                    if (!JobManagerException.isExpected(e)) {
                        logger.error("Unexpected error in job {} event stream", jobId, e);
                    } else {
                        logger.debug("Error in job {} event stream", jobId, e);
                    }
                });

        Subscription subscription = eventStream.subscribe(
                responseObserver::onNext,
                e -> responseObserver.onError(
                        new StatusRuntimeException(Status.INTERNAL
                                .withDescription(jobId + " job monitoring stream terminated with an error")
                                .withCause(e))
                ),
                responseObserver::onCompleted
        );

        ServerCallStreamObserver<JobChangeNotification> serverObserver = (ServerCallStreamObserver<JobChangeNotification>) responseObserver;
        serverObserver.setOnCancelHandler(subscription::unsubscribe);
    }

    private JobQueryResult toJobQueryResult(List<Job> jobs, Pagination runtimePagination) {
        return JobQueryResult.newBuilder()
                .addAllItems(jobs)
                .setPagination(toGrpcPagination(runtimePagination))
                .build();
    }

    private TaskQueryResult toTaskQueryResult(List<Task> tasks, Pagination runtimePagination) {
        return TaskQueryResult.newBuilder()
                .addAllItems(tasks)
                .setPagination(toGrpcPagination(runtimePagination))
                .build();
    }

    private ResourceDimension toResourceDimension(ContainerResources containerResources) {
        return ResourceDimension.newBuilder()
                .withCpus(containerResources.getCpu())
                .withGpu(containerResources.getGpu())
                .withMemoryMB(containerResources.getMemoryMB())
                .withDiskMB(containerResources.getDiskMB())
                .withNetworkMbs(containerResources.getNetworkMbps())
                .build();
    }

    private Tier findTier(com.netflix.titus.api.jobmanager.model.job.JobDescriptor jobDescriptor) {
        return JobManagerUtil.getTierAssignment(jobDescriptor, capacityGroupService).getLeft();
    }

    private List<ResourceDimension> getTierResourceLimits(Tier tier) {
        return agentManagementService.getInstanceGroups().stream()
                .filter(instanceGroup -> instanceGroup.getTier().equals(tier))
                .map(instanceGroup ->
                        agentManagementService.findResourceLimits(instanceGroup.getInstanceType()).orElse(instanceGroup.getResourceDimension())
                )
                .collect(Collectors.toList());
    }

    private boolean isTooLarge(ResourceDimension requestedResources, List<ResourceDimension> tierResourceLimits) {
        return tierResourceLimits.stream().noneMatch(limit -> ResourceDimensions.isBigger(limit, requestedResources));
    }

    private List<JobChangeNotification> createJobsSnapshot(
            Predicate<Pair<com.netflix.titus.api.jobmanager.model.job.Job<?>, List<com.netflix.titus.api.jobmanager.model.job.Task>>> jobsPredicate,
            Predicate<Pair<com.netflix.titus.api.jobmanager.model.job.Job<?>, com.netflix.titus.api.jobmanager.model.job.Task>> tasksPredicate) {
        List<JobChangeNotification> snapshot = new ArrayList<>();

        List<com.netflix.titus.api.jobmanager.model.job.Job<?>> coreJobs =
                jobOperations.findJobs(jobsPredicate, 0, Integer.MAX_VALUE / 2);
        coreJobs.forEach(coreJob -> snapshot.add(toJobChangeNotification(coreJob)));

        List<Pair<com.netflix.titus.api.jobmanager.model.job.Job<?>, com.netflix.titus.api.jobmanager.model.job.Task>> coreTasks =
                jobOperations.findTasks(tasksPredicate, 0, Integer.MAX_VALUE / 2);
        coreTasks.forEach(task -> snapshot.add(toJobChangeNotification(task.getRight())));

        return snapshot;
    }

    private List<JobChangeNotification> createJobSnapshot(String jobId) {
        List<JobChangeNotification> snapshot = new ArrayList<>();

        com.netflix.titus.api.jobmanager.model.job.Job<?> coreJob = jobOperations.getJob(jobId).orElseThrow(() -> new IllegalArgumentException("Job with id " + jobId + " not found"));
        snapshot.add(toJobChangeNotification(coreJob));

        List<com.netflix.titus.api.jobmanager.model.job.Task> coreTasks = jobOperations.getTasks(jobId);
        coreTasks.forEach(task -> snapshot.add(toJobChangeNotification(task)));

        return snapshot;
    }

    private JobChangeNotification toJobChangeNotification(com.netflix.titus.api.jobmanager.model.job.Job<?> coreJob) {
        Job grpcJob = V3GrpcModelConverters.toGrpcJob(coreJob);
        return JobChangeNotification.newBuilder()
                .setJobUpdate(JobChangeNotification.JobUpdate.newBuilder().setJob(grpcJob))
                .build();
    }

    private JobChangeNotification toJobChangeNotification(com.netflix.titus.api.jobmanager.model.job.Task coreTask) {
        com.netflix.titus.grpc.protogen.Task grpcTask = V3GrpcModelConverters.toGrpcTask(coreTask, logStorageInfo);
        return JobChangeNotification.newBuilder()
                .setTaskUpdate(JobChangeNotification.TaskUpdate.newBuilder().setTask(grpcTask))
                .build();
    }

}
