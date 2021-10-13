/*
 * Copyright 2019 Netflix, Inc.
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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.google.common.base.Stopwatch;
import com.google.protobuf.Empty;
import com.netflix.titus.api.jobmanager.model.job.CapacityAttributes;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.LogStorageInfo;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobProcesses;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudget;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.model.job.sanitizer.CustomJobConfiguration;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.jobmanager.service.V3JobOperations.Trigger;
import com.netflix.titus.api.model.Pagination;
import com.netflix.titus.api.model.PaginationUtil;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.api.model.callmetadata.CallMetadataConstants;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.model.admission.AdmissionSanitizer;
import com.netflix.titus.common.model.admission.AdmissionValidator;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.common.model.sanitizer.ValidationError;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.ExecutorsExt;
import com.netflix.titus.common.util.ProtobufExt;
import com.netflix.titus.common.util.archaius2.ObjectConfigurationResolver;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobAttributesDeleteRequest;
import com.netflix.titus.grpc.protogen.JobAttributesUpdate;
import com.netflix.titus.grpc.protogen.JobCapacityUpdate;
import com.netflix.titus.grpc.protogen.JobCapacityUpdateWithOptionalAttributes;
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
import com.netflix.titus.grpc.protogen.TaskAttributesDeleteRequest;
import com.netflix.titus.grpc.protogen.TaskAttributesUpdate;
import com.netflix.titus.grpc.protogen.TaskId;
import com.netflix.titus.grpc.protogen.TaskKillRequest;
import com.netflix.titus.grpc.protogen.TaskMoveRequest;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import com.netflix.titus.grpc.protogen.TaskStatus;
import com.netflix.titus.master.config.CellInfoResolver;
import com.netflix.titus.master.endpoint.common.CellDecorator;
import com.netflix.titus.master.endpoint.grpc.GrpcMasterEndpointConfiguration;
import com.netflix.titus.runtime.endpoint.JobQueryCriteria;
import com.netflix.titus.runtime.endpoint.authorization.AuthorizationService;
import com.netflix.titus.runtime.endpoint.authorization.AuthorizationStatus;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataUtils;
import com.netflix.titus.runtime.endpoint.v3.grpc.DefaultGrpcObjectsCache;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcObjectsCacheConfiguration;
import com.netflix.titus.runtime.endpoint.v3.grpc.query.V3JobQueryCriteriaEvaluator;
import com.netflix.titus.runtime.endpoint.v3.grpc.query.V3TaskQueryCriteriaEvaluator;
import com.netflix.titus.runtime.jobmanager.JobComparators;
import com.netflix.titus.runtime.jobmanager.JobManagerCursors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.schedulers.Schedulers;

import static com.netflix.titus.api.jobmanager.model.job.sanitizer.JobSanitizerBuilder.JOB_STRICT_SANITIZER;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.safeOnError;
import static com.netflix.titus.runtime.endpoint.metadata.CallMetadataUtils.execute;
import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobQueryModelConverters.toGrpcPagination;
import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobQueryModelConverters.toJobQueryCriteria;
import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobQueryModelConverters.toPage;
import static com.netflix.titus.runtime.endpoint.v3.grpc.TitusPaginationUtils.checkPageIsValid;
import static com.netflix.titus.runtime.jobmanager.gateway.JobServiceGateway.JOB_MINIMUM_FIELD_SET;
import static com.netflix.titus.runtime.jobmanager.gateway.JobServiceGateway.TASK_MINIMUM_FIELD_SET;

@Singleton
public class DefaultJobManagementServiceGrpc extends JobManagementServiceGrpc.JobManagementServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(DefaultJobManagementServiceGrpc.class);

    private static final JobChangeNotification SNAPSHOT_END_MARKER = JobChangeNotification.newBuilder()
            .setSnapshotEnd(JobChangeNotification.SnapshotEnd.newBuilder())
            .build();

    private final V3JobOperations jobOperations;
    private final EntitySanitizer entitySanitizer;
    private final AdmissionValidator<com.netflix.titus.api.jobmanager.model.job.JobDescriptor> admissionValidator;
    private final AdmissionSanitizer<com.netflix.titus.api.jobmanager.model.job.JobDescriptor> admissionSanitizer;
    private final ObjectConfigurationResolver<com.netflix.titus.api.jobmanager.model.job.JobDescriptor, CustomJobConfiguration> customJobConfigurationResolver;
    private final CallMetadataResolver callMetadataResolver;
    private final CellDecorator cellDecorator;
    private final AuthorizationService authorizationService;
    private final TitusRuntime titusRuntime;
    private final Scheduler observeJobsScheduler;
    private final DefaultGrpcObjectsCache grpcObjectsCache;
    private final DefaultJobManagementServiceGrpcMetrics metrics;

    @Inject
    public DefaultJobManagementServiceGrpc(GrpcMasterEndpointConfiguration configuration,
                                           V3JobOperations jobOperations,
                                           LogStorageInfo<com.netflix.titus.api.jobmanager.model.job.Task> logStorageInfo,
                                           @Named(JOB_STRICT_SANITIZER) EntitySanitizer entitySanitizer,
                                           AdmissionValidator<com.netflix.titus.api.jobmanager.model.job.JobDescriptor> admissionValidator,
                                           AdmissionSanitizer<com.netflix.titus.api.jobmanager.model.job.JobDescriptor> admissionSanitizer,
                                           ObjectConfigurationResolver<com.netflix.titus.api.jobmanager.model.job.JobDescriptor, CustomJobConfiguration> customJobConfigurationResolver,
                                           CallMetadataResolver callMetadataResolver,
                                           CellInfoResolver cellInfoResolver,
                                           AuthorizationService authorizationService,
                                           GrpcObjectsCacheConfiguration grpcObjectsCacheConfiguration,
                                           TitusRuntime titusRuntime) {
        this.jobOperations = jobOperations;
        this.entitySanitizer = entitySanitizer;
        this.admissionValidator = admissionValidator;
        this.admissionSanitizer = admissionSanitizer;
        this.customJobConfigurationResolver = customJobConfigurationResolver;
        this.callMetadataResolver = callMetadataResolver;
        this.cellDecorator = new CellDecorator(cellInfoResolver::getCellName);
        this.authorizationService = authorizationService;
        this.titusRuntime = titusRuntime;
        this.observeJobsScheduler = Schedulers.from(ExecutorsExt.instrumentedFixedSizeThreadPool(
                titusRuntime.getRegistry(), "observeJobs", configuration.getServerStreamsThreadPoolSize()));

        this.grpcObjectsCache = new DefaultGrpcObjectsCache(jobOperations, grpcObjectsCacheConfiguration, logStorageInfo, titusRuntime);
        grpcObjectsCache.activate();
        this.metrics = new DefaultJobManagementServiceGrpcMetrics(titusRuntime);
    }

    @PreDestroy
    public void shutdown() {
        metrics.shutdown();
        grpcObjectsCache.shutdown();
    }

    @Override
    public void createJob(JobDescriptor jobDescriptor, StreamObserver<JobId> responseObserver) {
        execute(callMetadataResolver, responseObserver, callMetadata -> validateAndConvertJobDescriptorToCoreModel(jobDescriptor)
                .flatMap(sanitizedCoreJobDescriptor -> authorizeJobCreate(callMetadata, sanitizedCoreJobDescriptor)
                        .then(Mono.just(sanitizedCoreJobDescriptor)))
                .flatMap(sanitizedCoreJobDescriptor -> jobOperations.createJobReactor(sanitizedCoreJobDescriptor, callMetadata))
                .subscribe(
                        jobId -> responseObserver.onNext(JobId.newBuilder().setId(jobId).build()),
                        e -> safeOnError(logger, e, responseObserver),
                        responseObserver::onCompleted
                ));
    }

    private Mono<com.netflix.titus.api.jobmanager.model.job.JobDescriptor> validateAndConvertJobDescriptorToCoreModel(JobDescriptor jobDescriptor) {
        return Mono.defer(() -> {
            com.netflix.titus.api.jobmanager.model.job.JobDescriptor coreJobDescriptor;
            try {
                coreJobDescriptor = GrpcJobManagementModelConverters.toCoreJobDescriptor(cellDecorator.ensureCellInfo(jobDescriptor));
            } catch (Exception e) {
                return Mono.error(TitusServiceException.invalidArgument("Error when converting GRPC object to the internal representation: " + e.getMessage()));
            }

            return Mono.fromCallable(() -> entitySanitizer.sanitize(coreJobDescriptor).orElse(coreJobDescriptor))
                    .flatMap(admissionSanitizer::sanitizeAndApply)
                    .flatMap(sanitizedCoreJobDescriptor -> admissionValidator.validate(sanitizedCoreJobDescriptor)
                            .map(violations -> CollectionsExt.merge(
                                    violations,
                                    entitySanitizer.validate(sanitizedCoreJobDescriptor),
                                    validateCustomJobLimits(sanitizedCoreJobDescriptor)))
                            .flatMap(violations -> {
                                if (!violations.isEmpty()) {
                                    return Mono.error(TitusServiceException.invalidArgument(violations));
                                }
                                return Mono.just(sanitizedCoreJobDescriptor);
                            }));
        });
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

            Pair<List<com.netflix.titus.api.jobmanager.model.job.Job<?>>, Pagination> queryResult = PaginationUtil.takePageWithCursorAndKeyExtractor(
                    toPage(jobQuery.getPage()),
                    allFilteredJobs,
                    JobComparators::createJobKeyOf,
                    JobManagerCursors::coreJobIndexOf,
                    JobManagerCursors::newJobCoreCursorFrom
            );
            List<Job> grpcJobs = new ArrayList<>();
            for (com.netflix.titus.api.jobmanager.model.job.Job<?> job : queryResult.getLeft()) {
                Job toGrpcJob = grpcObjectsCache.getJob(job);
                grpcJobs.add(toGrpcJob);
            }

            JobQueryResult grpcQueryResult;
            if (jobQuery.getFieldsList().isEmpty()) {
                grpcQueryResult = toJobQueryResult(grpcJobs, queryResult.getRight());
            } else {
                Set<String> fields = new HashSet<>(jobQuery.getFieldsList());
                fields.addAll(JOB_MINIMUM_FIELD_SET);
                List<Job> list = new ArrayList<>();
                for (Job j : grpcJobs) {
                    list.add(ProtobufExt.copy(j, fields));
                }
                grpcQueryResult = toJobQueryResult(list, queryResult.getRight());
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
            com.netflix.titus.api.jobmanager.model.job.Job<?> coreJob = jobOperations.getJob(id).orElse(null);
            if (coreJob == null) {
                safeOnError(logger, JobManagerException.jobNotFound(id), responseObserver);
            } else {
                responseObserver.onNext(grpcObjectsCache.getJob(coreJob));
                responseObserver.onCompleted();
            }
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
            List<com.netflix.titus.api.jobmanager.model.job.Task> allFilteredTasks = new ArrayList<>();
            for (Pair<com.netflix.titus.api.jobmanager.model.job.Job<?>, com.netflix.titus.api.jobmanager.model.job.Task> jobTaskPair : jobOperations.findTasks(
                    new V3TaskQueryCriteriaEvaluator(toJobQueryCriteria(taskQuery), titusRuntime),
                    0,
                    Integer.MAX_VALUE / 2
            )) {
                com.netflix.titus.api.jobmanager.model.job.Task right = jobTaskPair.getRight();
                allFilteredTasks.add(right);
            }

            Pair<List<com.netflix.titus.api.jobmanager.model.job.Task>, Pagination> queryResult = PaginationUtil.takePageWithCursorAndKeyExtractor(
                    toPage(taskQuery.getPage()),
                    allFilteredTasks,
                    JobComparators::createTaskKeyOf,
                    JobManagerCursors::coreTaskIndexOf,
                    JobManagerCursors::newTaskCoreCursorFrom
            );

            List<Task> grpcTasks = new ArrayList<>();
            for (com.netflix.titus.api.jobmanager.model.job.Task task : queryResult.getLeft()) {
                Task toGrpcTask = grpcObjectsCache.getTask(task);
                grpcTasks.add(toGrpcTask);
            }

            TaskQueryResult grpcQueryResult;
            if (taskQuery.getFieldsList().isEmpty()) {
                grpcQueryResult = toTaskQueryResult(grpcTasks, queryResult.getRight());
            } else {
                Set<String> fields = new HashSet<>(taskQuery.getFieldsList());
                fields.addAll(TASK_MINIMUM_FIELD_SET);
                List<Task> filtered = new ArrayList<>();
                for (Task t : grpcTasks) {
                    filtered.add(ProtobufExt.copy(t, fields));
                }
                grpcQueryResult = toTaskQueryResult(filtered, queryResult.getRight());
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
            Pair<com.netflix.titus.api.jobmanager.model.job.Job<?>, com.netflix.titus.api.jobmanager.model.job.Task> coreJobAndTask = jobOperations.findTaskById(id).orElse(null);
            if (coreJobAndTask == null) {
                safeOnError(logger, JobManagerException.taskNotFound(id), responseObserver);
            } else {
                com.netflix.titus.api.jobmanager.model.job.Task coreTask = coreJobAndTask.getRight();
                Task grpcTask = grpcObjectsCache.getTask(coreTask);
                responseObserver.onNext(grpcTask);
                responseObserver.onCompleted();
            }
        } catch (Exception e) {
            safeOnError(logger, e, responseObserver);
        }
    }

    @Override
    public void updateJobCapacity(JobCapacityUpdate request, StreamObserver<Empty> responseObserver) {
        execute(callMetadataResolver, responseObserver, callMetadata -> {
            CapacityAttributes capacityAttributes = GrpcJobManagementModelConverters.toCoreCapacityAttributes(request.getCapacity());
            verifyServiceJob(request.getJobId(), capacityAttributes);

            authorizeJobUpdate(callMetadata, request.getJobId())
                    .concatWith(jobOperations.updateJobCapacityAttributesReactor(request.getJobId(), capacityAttributes, callMetadata))
                    .subscribe(
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
    public void updateJobCapacityWithOptionalAttributes(JobCapacityUpdateWithOptionalAttributes request, StreamObserver<Empty> responseObserver) {
        execute(callMetadataResolver, responseObserver, callMetadata -> {
            CapacityAttributes capacityAttributes = GrpcJobManagementModelConverters.toCoreCapacityAttributes(request.getJobCapacityWithOptionalAttributes());
            verifyServiceJob(request.getJobId(), capacityAttributes);
            logger.info("updateJobCapacityWithOptionalAttributes to {}", capacityAttributes);

            authorizeJobUpdate(callMetadata, request.getJobId())
                    .concatWith(jobOperations.updateJobCapacityAttributesReactor(request.getJobId(), capacityAttributes, callMetadata))
                    .subscribe(
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
            ServiceJobProcesses serviceJobProcesses = GrpcJobManagementModelConverters.toCoreServiceJobProcesses(request.getServiceJobProcesses());

            authorizeJobUpdate(callMetadata, request.getJobId())
                    .concatWith(jobOperations.updateServiceJobProcessesReactor(request.getJobId(), serviceJobProcesses, callMetadata))
                    .subscribe(
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
        execute(callMetadataResolver, responseObserver, callMetadata -> authorizeJobUpdate(callMetadata, request.getId())
                .concatWith(jobOperations.updateJobStatusReactor(request.getId(), request.getEnableStatus(), callMetadata))
                .subscribe(
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

                    authorizeJobUpdate(callMetadata, job)
                            .concatWith(jobOperations.updateJobDisruptionBudget(request.getJobId(), sanitized, callMetadata))
                            .subscribe(
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
        DisruptionBudget coreDisruptionBudget;
        try {
            coreDisruptionBudget = GrpcJobManagementModelConverters.toCoreDisruptionBudget(grpcDisruptionBudget);
        } catch (Exception e) {
            safeOnError(logger, TitusServiceException.invalidArgument("Error when converting GRPC disruption budget to the internal model: " + e.getMessage()), responseObserver);
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
        execute(callMetadataResolver, responseObserver, callMetadata -> {
            String reason = String.format("User initiated job kill: %s", CallMetadataUtils.toReasonString(callMetadata));
            authorizeJobUpdate(callMetadata, request.getId())
                    .concatWith(jobOperations.killJobReactor(request.getId(), reason, callMetadata))
                    .subscribe(
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
    public void updateJobAttributes(JobAttributesUpdate request, StreamObserver<Empty> responseObserver) {
        execute(callMetadataResolver, responseObserver, callMetadata -> authorizeJobUpdate(callMetadata, request.getJobId())
                .concatWith(jobOperations.updateJobAttributes(request.getJobId(), request.getAttributesMap(), callMetadata))
                .subscribe(
                        nothing -> {
                        },
                        e -> safeOnError(logger, e, responseObserver),
                        () -> {
                            responseObserver.onNext(Empty.getDefaultInstance());
                            responseObserver.onCompleted();
                        }
                )
        );
    }

    @Override
    public void deleteJobAttributes(JobAttributesDeleteRequest request, StreamObserver<Empty> responseObserver) {
        execute(callMetadataResolver, responseObserver, callMetadata -> authorizeJobUpdate(callMetadata, request.getJobId())
                .concatWith(jobOperations.deleteJobAttributes(request.getJobId(), new HashSet<>(request.getKeysList()), callMetadata))
                .subscribe(
                        nothing -> {
                        },
                        e -> safeOnError(logger, e, responseObserver),
                        () -> {
                            responseObserver.onNext(Empty.getDefaultInstance());
                            responseObserver.onCompleted();
                        }
                )
        );
    }

    @Override
    public void killTask(TaskKillRequest request, StreamObserver<Empty> responseObserver) {
        execute(callMetadataResolver, responseObserver, callMetadata -> authorizeTaskUpdate(callMetadata, request.getTaskId())
                .concatWith(jobOperations.killTask(request.getTaskId(), request.getShrink(), request.getPreventMinSizeUpdate(), Trigger.API, callMetadata))
                .subscribe(
                        nothing -> {
                        },
                        e -> safeOnError(logger, e, responseObserver),
                        () -> {
                            responseObserver.onNext(Empty.getDefaultInstance());
                            responseObserver.onCompleted();
                        }
                )
        );
    }

    @Override
    public void moveTask(TaskMoveRequest request, StreamObserver<Empty> responseObserver) {
        execute(callMetadataResolver, responseObserver, callMetadata ->
                jobOperations.moveServiceTask(request.getSourceJobId(), request.getTargetJobId(), request.getTaskId(), callMetadata).subscribe(
                        nothing -> {
                        },
                        e -> safeOnError(logger, e, responseObserver),
                        () -> {
                            responseObserver.onNext(Empty.getDefaultInstance());
                            responseObserver.onCompleted();
                        }
                )
        );
    }

    @Override
    public void updateTaskAttributes(TaskAttributesUpdate request, StreamObserver<Empty> responseObserver) {
        execute(callMetadataResolver, responseObserver, callMetadata -> jobOperations.updateTask(
                request.getTaskId(),
                task -> {
                    Map<String, String> updatedAttributes = CollectionsExt.merge(task.getAttributes(), request.getAttributesMap());
                    return Optional.of(task.toBuilder().withAttributes(updatedAttributes).build());
                },
                Trigger.API,
                "User request: userId=" + callMetadata.getCallerId(), callMetadata
        ).subscribe(
                () -> {
                    responseObserver.onNext(Empty.getDefaultInstance());
                    responseObserver.onCompleted();
                },
                e -> safeOnError(logger, e, responseObserver)
        ));
    }

    @Override
    public void deleteTaskAttributes(TaskAttributesDeleteRequest request, StreamObserver<Empty> responseObserver) {
        execute(callMetadataResolver, responseObserver, callMetadata -> jobOperations.updateTask(
                request.getTaskId(),
                task -> {
                    Map<String, String> updatedAttributes = CollectionsExt.copyAndRemove(task.getAttributes(), request.getKeysList());
                    return Optional.of(task.toBuilder().withAttributes(updatedAttributes).build());
                },
                Trigger.API,
                "User request: userId=" + callMetadata.getCallerId(),
                callMetadata
        ).subscribe(
                () -> {
                    responseObserver.onNext(Empty.getDefaultInstance());
                    responseObserver.onCompleted();
                },
                e -> safeOnError(logger, e, responseObserver)
        ));
    }

    @Override
    public void observeJobs(ObserveJobsQuery query, StreamObserver<JobChangeNotification> responseObserver) {
        Stopwatch start = Stopwatch.createStarted();

        String trxId = UUID.randomUUID().toString();
        CallMetadata callMetadata = callMetadataResolver.resolve().orElse(CallMetadataConstants.UNDEFINED_CALL_METADATA);
        metrics.observeJobsStarted(trxId, callMetadata);

        JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> criteria = toJobQueryCriteria(query);
        V3JobQueryCriteriaEvaluator jobsPredicate = new V3JobQueryCriteriaEvaluator(criteria, titusRuntime);
        V3TaskQueryCriteriaEvaluator tasksPredicate = new V3TaskQueryCriteriaEvaluator(criteria, titusRuntime);

        Observable<JobChangeNotification> eventStream = jobOperations.observeJobs(jobsPredicate, tasksPredicate)
                // avoid clogging the computation scheduler
                .observeOn(observeJobsScheduler)
                .subscribeOn(observeJobsScheduler, false)
                .map(event -> GrpcJobManagementModelConverters.toGrpcJobChangeNotification(event, grpcObjectsCache, titusRuntime.getClock().wallTime()))
                .compose(ObservableExt.head(() -> {
                    List<JobChangeNotification> snapshot = createJobsSnapshot(jobsPredicate, tasksPredicate);
                    snapshot.add(SNAPSHOT_END_MARKER);
                    return snapshot;
                }))
                .doOnError(e -> logger.error("Unexpected error in jobs event stream", e));

        AtomicBoolean closingProcessed = new AtomicBoolean();
        Subscription subscription = eventStream
                .doOnUnsubscribe(() -> {
                    if (!closingProcessed.getAndSet(true)) {
                        metrics.observeJobsUnsubscribed(trxId, start.elapsed(TimeUnit.MILLISECONDS));
                    }
                })
                .subscribe(
                        event -> {
                            metrics.observeJobsEventEmitted(trxId);
                            responseObserver.onNext(event);
                        },
                        e -> {
                            if (!closingProcessed.getAndSet(true)) {
                                metrics.observeJobsError(trxId, start.elapsed(TimeUnit.MILLISECONDS), e);
                            }
                            responseObserver.onError(
                                    new StatusRuntimeException(Status.INTERNAL
                                            .withDescription("All jobs monitoring stream terminated with an error")
                                            .withCause(e))
                            );
                        },
                        () -> {
                            if (!closingProcessed.getAndSet(true)) {
                                metrics.observeJobsCompleted(trxId, start.elapsed(TimeUnit.MILLISECONDS));
                            }
                            responseObserver.onCompleted();
                        }
                );

        ServerCallStreamObserver<JobChangeNotification> serverObserver = (ServerCallStreamObserver<JobChangeNotification>) responseObserver;
        serverObserver.setOnCancelHandler(subscription::unsubscribe);
    }

    @Override
    public void observeJob(JobId request, StreamObserver<JobChangeNotification> responseObserver) {
        String jobId = request.getId();
        Observable<JobChangeNotification> eventStream = jobOperations.observeJob(jobId)
                // avoid clogging the computation scheduler
                .observeOn(observeJobsScheduler)
                .subscribeOn(observeJobsScheduler, false)
                .map(event -> GrpcJobManagementModelConverters.toGrpcJobChangeNotification(event, grpcObjectsCache, titusRuntime.getClock().wallTime()))
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

    private Mono<Void> authorizeJobCreate(CallMetadata callMetadata, com.netflix.titus.api.jobmanager.model.job.JobDescriptor<?> jobDescriptor) {
        return authorizationService.authorize(callMetadata, jobDescriptor).flatMap(this::processAuthorizationReply);
    }

    private Mono<Void> authorizeJobUpdate(CallMetadata callMetadata, String jobId) {
        return Mono.defer(() -> {
            com.netflix.titus.api.jobmanager.model.job.Job<?> job = jobOperations.getJob(jobId).orElse(null);
            if (job == null) {
                return Mono.error(JobManagerException.jobNotFound(jobId));
            }
            return authorizeJobUpdate(callMetadata, job);
        });
    }

    private Mono<Void> authorizeJobUpdate(CallMetadata callMetadata, com.netflix.titus.api.jobmanager.model.job.Job<?> job) {
        return authorizationService.authorize(callMetadata, job).flatMap(this::processAuthorizationReply);
    }

    private Mono<Void> authorizeTaskUpdate(CallMetadata callMetadata, String taskId) {
        return Mono.defer(() -> {
            Pair<com.netflix.titus.api.jobmanager.model.job.Job<?>, com.netflix.titus.api.jobmanager.model.job.Task> jobTaskPair = jobOperations.findTaskById(taskId).orElse(null);
            if (jobTaskPair == null) {
                return Mono.error(JobManagerException.taskNotFound(taskId));
            }
            return authorizeJobUpdate(callMetadata, jobTaskPair.getLeft());
        });
    }

    private Mono<Void> processAuthorizationReply(AuthorizationStatus authorizationResult) {
        if (!authorizationResult.isAuthorized()) {
            Status status = Status.PERMISSION_DENIED
                    .withDescription("Request not authorized: " + authorizationResult.getReason());
            return Mono.error(new StatusRuntimeException(status));
        }
        return Mono.empty();
    }

    private Set<ValidationError> validateCustomJobLimits(com.netflix.titus.api.jobmanager.model.job.JobDescriptor jobDescriptor) {
        CustomJobConfiguration config = customJobConfigurationResolver.resolve(jobDescriptor);

        if (JobFunctions.isServiceJob(jobDescriptor)) {
            ServiceJobExt ext = (ServiceJobExt) jobDescriptor.getExtensions();
            if (ext.getCapacity().getMax() > config.getMaxServiceJobSize()) {
                String message = String.format("Service job size is limited to %s, but is %s", config.getMaxServiceJobSize(), ext.getCapacity().getMax());
                return Collections.singleton(new ValidationError("jobDescriptor.extensions.capacity.max", message));
            }
        } else {
            BatchJobExt ext = (BatchJobExt) jobDescriptor.getExtensions();
            if (ext.getSize() > config.getMaxBatchJobSize()) {
                String message = String.format("Batch job size is limited to %s, but is %s", config.getMaxBatchJobSize(), ext.getSize());
                return Collections.singleton(new ValidationError("jobDescriptor.extensions.size", message));
            }
        }

        return Collections.emptySet();
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

    private List<JobChangeNotification> createJobsSnapshot(
            Predicate<Pair<com.netflix.titus.api.jobmanager.model.job.Job<?>, List<com.netflix.titus.api.jobmanager.model.job.Task>>> jobsPredicate,
            Predicate<Pair<com.netflix.titus.api.jobmanager.model.job.Job<?>, com.netflix.titus.api.jobmanager.model.job.Task>> tasksPredicate) {
        long now = titusRuntime.getClock().wallTime();
        List<JobChangeNotification> snapshot = new ArrayList<>();

        // Generics casting issue
        List allJobsAndTasksRaw = jobOperations.getJobsAndTasks();
        List<Pair<com.netflix.titus.api.jobmanager.model.job.Job<?>, List<com.netflix.titus.api.jobmanager.model.job.Task>>> allJobsAndTasks = allJobsAndTasksRaw;
        allJobsAndTasks.forEach(pair -> {
            com.netflix.titus.api.jobmanager.model.job.Job<?> job = pair.getLeft();
            List<com.netflix.titus.api.jobmanager.model.job.Task> tasks = pair.getRight();
            if (jobsPredicate.test(pair)) {
                snapshot.add(toJobChangeNotification(job, now));
            }
            tasks.forEach(task -> {
                if (tasksPredicate.test(Pair.of(job, task))) {
                    snapshot.add(toJobChangeNotification(task, now));
                }
            });
        });

        return snapshot;
    }

    private List<JobChangeNotification> createJobSnapshot(String jobId) {
        long now = titusRuntime.getClock().wallTime();
        List<JobChangeNotification> snapshot = new ArrayList<>();

        com.netflix.titus.api.jobmanager.model.job.Job<?> coreJob = jobOperations.getJob(jobId).orElseThrow(() -> new IllegalArgumentException("Job with id " + jobId + " not found"));
        snapshot.add(toJobChangeNotification(coreJob, now));

        List<com.netflix.titus.api.jobmanager.model.job.Task> coreTasks = jobOperations.getTasks(jobId);
        coreTasks.forEach(task -> snapshot.add(toJobChangeNotification(task, now)));

        return snapshot;
    }

    private JobChangeNotification toJobChangeNotification(com.netflix.titus.api.jobmanager.model.job.Job<?> coreJob, long now) {
        Job grpcJob = grpcObjectsCache.getJob(coreJob);
        return JobChangeNotification.newBuilder()
                .setJobUpdate(JobChangeNotification.JobUpdate.newBuilder().setJob(grpcJob))
                .setTimestamp(now)
                .build();
    }

    private JobChangeNotification toJobChangeNotification(com.netflix.titus.api.jobmanager.model.job.Task coreTask, long now) {
        com.netflix.titus.grpc.protogen.Task grpcTask = grpcObjectsCache.getTask(coreTask);
        return JobChangeNotification.newBuilder()
                .setTaskUpdate(JobChangeNotification.TaskUpdate.newBuilder().setTask(grpcTask))
                .setTimestamp(now)
                .build();
    }

    private com.netflix.titus.api.jobmanager.model.job.Job<ServiceJobExt> verifyServiceJob(String jobId, CapacityAttributes capacityAttributes) {
        return jobOperations
                .getJob(jobId)
                .map(j -> {
                    if (!JobFunctions.isServiceJob(j)) {
                        throw JobManagerException.notServiceJob(j.getId());
                    } else if (j.getJobDescriptor().getContainer().getContainerResources().getSignedIpAddressAllocations().size() > 0 &&
                            capacityAttributes.getMax().orElse(0) > j.getJobDescriptor().getContainer().getContainerResources().getSignedIpAddressAllocations().size()) {
                        throw JobManagerException.invalidMaxCapacity(
                                j.getId(),
                                capacityAttributes.getMax().orElse(0),
                                j.getJobDescriptor().getContainer().getContainerResources().getSignedIpAddressAllocations().size());
                    } else if (j.getJobDescriptor().getContainer().getContainerResources().getEbsVolumes().size() > 0 &&
                            capacityAttributes.getMax().orElse(0) > j.getJobDescriptor().getContainer().getContainerResources().getEbsVolumes().size()) {
                        throw JobManagerException.invalidMaxCapacity(
                                j.getId(),
                                capacityAttributes.getMax().orElse(0),
                                j.getJobDescriptor().getContainer().getContainerResources().getEbsVolumes().size());
                    }
                    return (com.netflix.titus.api.jobmanager.model.job.Job<ServiceJobExt>) j;
                })
                .orElseThrow(() -> JobManagerException.jobNotFound(jobId));
    }
}
