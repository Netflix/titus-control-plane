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

package io.netflix.titus.gateway.service.v3.internal;


import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.validation.ConstraintViolation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobCapacityUpdate;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceStub;
import com.netflix.titus.grpc.protogen.JobProcessesUpdate;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.JobStatusUpdate;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskId;
import com.netflix.titus.grpc.protogen.TaskKillRequest;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import io.grpc.ClientCall;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.netflix.titus.api.jobmanager.model.job.Capacity;
import io.netflix.titus.api.jobmanager.model.job.JobFunctions;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.store.JobStore;
import io.netflix.titus.api.jobmanager.store.JobStoreException;
import io.netflix.titus.api.model.Page;
import io.netflix.titus.api.model.Pagination;
import io.netflix.titus.api.model.PaginationUtil;
import io.netflix.titus.api.service.TitusServiceException;
import io.netflix.titus.common.grpc.GrpcUtil;
import io.netflix.titus.common.grpc.SessionContext;
import io.netflix.titus.common.model.sanitizer.EntitySanitizer;
import io.netflix.titus.common.util.ExceptionExt;
import io.netflix.titus.common.util.StringExt;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.gateway.service.v3.GrpcClientConfiguration;
import io.netflix.titus.gateway.service.v3.JobManagementService;
import io.netflix.titus.runtime.endpoint.common.LogStorageInfo;
import io.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Emitter;
import rx.Observable;
import rx.functions.Action1;

import static com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.METHOD_CREATE_JOB;
import static com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.METHOD_FIND_JOB;
import static com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.METHOD_FIND_JOBS;
import static com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.METHOD_FIND_TASK;
import static com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.METHOD_FIND_TASKS;
import static com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.METHOD_KILL_JOB;
import static com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.METHOD_KILL_TASK;
import static com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.METHOD_OBSERVE_JOB;
import static com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.METHOD_OBSERVE_JOBS;
import static com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.METHOD_UPDATE_JOB_CAPACITY;
import static com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.METHOD_UPDATE_JOB_PROCESSES;
import static com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.METHOD_UPDATE_JOB_STATUS;
import static io.netflix.titus.common.grpc.GrpcUtil.createSimpleStreamObserver;
import static io.netflix.titus.gateway.service.v3.internal.GrpcServiceUtil.getRxJavaAdjustedTimeout;
import static io.netflix.titus.runtime.TitusEntitySanitizerModule.JOB_SANITIZER;
import static io.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toGrpcPagination;

@Singleton
public class DefaultJobManagementService implements JobManagementService {
    private static Logger logger = LoggerFactory.getLogger(DefaultAutoScalingService.class);

    private static final int MAX_CONCURRENT_JOBS_TO_RETRIEVE = 10;

    private final GrpcClientConfiguration configuration;
    private final JobManagementServiceStub client;
    private final SessionContext sessionContext;
    private final JobStore store;
    private final LogStorageInfo<io.netflix.titus.api.jobmanager.model.job.Task> logStorageInfo;
    private final EntitySanitizer entitySanitizer;

    @Inject
    public DefaultJobManagementService(GrpcClientConfiguration configuration,
                                       JobManagementServiceStub client,
                                       SessionContext sessionContext,
                                       JobStore store,
                                       LogStorageInfo<io.netflix.titus.api.jobmanager.model.job.Task> logStorageInfo,
                                       @Named(JOB_SANITIZER) EntitySanitizer entitySanitizer) {
        this.configuration = configuration;
        this.client = client;
        this.sessionContext = sessionContext;
        this.store = store;
        this.logStorageInfo = logStorageInfo;
        this.entitySanitizer = entitySanitizer;
    }

    @Override
    public Observable<String> createJob(JobDescriptor jobDescriptor) {
        io.netflix.titus.api.jobmanager.model.job.JobDescriptor coreJobDescriptor;
        try {
            coreJobDescriptor = V3GrpcModelConverters.toCoreJobDescriptor(jobDescriptor);
        } catch (Exception e) {
            return Observable.error(TitusServiceException.invalidArgument(e));
        }
        io.netflix.titus.api.jobmanager.model.job.JobDescriptor sanitizedCoreJobDescriptor = entitySanitizer.sanitize(coreJobDescriptor).orElse(coreJobDescriptor);

        Set<ConstraintViolation<io.netflix.titus.api.jobmanager.model.job.JobDescriptor>> violations = entitySanitizer.validate(sanitizedCoreJobDescriptor);
        if (!violations.isEmpty()) {
            return Observable.error(TitusServiceException.invalidArgument(violations));
        }
        return toObservable(emitter -> {
            final Action1<? super JobId> onNext = value -> emitter.onNext(value.getId());
            StreamObserver<JobId> streamObserver = GrpcUtil.createStreamObserver(onNext, emitter::onError, emitter::onCompleted);
            ClientCall clientCall = call(METHOD_CREATE_JOB, jobDescriptor, streamObserver);
            GrpcUtil.attachCancellingCallback(emitter, clientCall);
        });
    }

    @Override
    public Completable updateJobCapacity(JobCapacityUpdate jobCapacityUpdate) {
        Capacity newCapacity = V3GrpcModelConverters.toCoreCapacity(jobCapacityUpdate.getCapacity());
        Set<ConstraintViolation<Capacity>> violations = entitySanitizer.validate(newCapacity);
        if (!violations.isEmpty()) {
            return Completable.error(TitusServiceException.invalidArgument(violations));
        }
        return toCompletable(
                emitter -> {
                    StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyStreamObserver(emitter);
                    ClientCall clientCall = call(METHOD_UPDATE_JOB_CAPACITY, jobCapacityUpdate, streamObserver);
                    GrpcUtil.attachCancellingCallback(emitter, clientCall);
                });
    }

    @Override
    public Completable updateJobProcesses(JobProcessesUpdate jobProcessesUpdate) {
        return toCompletable(
                emitter -> {
                    StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyStreamObserver(emitter);
                    ClientCall clientCall = call(METHOD_UPDATE_JOB_PROCESSES, jobProcessesUpdate, streamObserver);
                    GrpcUtil.attachCancellingCallback(emitter, clientCall);
                }
        );
    }

    @Override
    public Completable changeJobInServiceStatus(JobStatusUpdate statusUpdate) {
        return toCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyStreamObserver(emitter);
            ClientCall clientCall = call(METHOD_UPDATE_JOB_STATUS, statusUpdate, streamObserver);
            GrpcUtil.attachCancellingCallback(emitter, clientCall);
        });
    }

    @Override
    public Observable<Job> findJob(String jobId) {
        Observable<Job> observable = Observable.create(emitter -> {
            StreamObserver<Job> streamObserver = createSimpleStreamObserver(emitter);
            ClientCall clientCall = call(METHOD_FIND_JOB, JobId.newBuilder().setId(jobId).build(), streamObserver);
            GrpcUtil.attachCancellingCallback(emitter, clientCall);
        }, Emitter.BackpressureMode.NONE);

        observable = observable.onErrorResumeNext(e -> {
            if (e instanceof StatusRuntimeException &&
                    ((StatusRuntimeException) e).getStatus().getCode() == Status.Code.NOT_FOUND) {
                return retrieveArchivedJob(jobId);
            } else {
                return Observable.error(e);
            }
        });

        return observable.timeout(configuration.getRequestTimeout(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Observable<JobQueryResult> findJobs(JobQuery jobQuery) {
        return toObservable(emitter -> {
            StreamObserver<JobQueryResult> streamObserver = createSimpleStreamObserver(emitter);
            ClientCall clientCall = call(METHOD_FIND_JOBS, jobQuery, streamObserver);
            GrpcUtil.attachCancellingCallback(emitter, clientCall);
        });
    }

    @Override
    public Observable<JobChangeNotification> observeJob(String jobId) {
        return Observable.create(emitter -> {
            StreamObserver<JobChangeNotification> streamObserver = createSimpleStreamObserver(emitter);
            ClientCall clientCall = callStreaming(METHOD_OBSERVE_JOB, JobId.newBuilder().setId(jobId).build(), streamObserver);
            GrpcUtil.attachCancellingCallback(emitter, clientCall);
        }, Emitter.BackpressureMode.NONE);
    }

    @Override
    public Observable<JobChangeNotification> observeJobs() {
        return Observable.create(emitter -> {
            StreamObserver<JobChangeNotification> streamObserver = createSimpleStreamObserver(emitter);
            ClientCall clientCall = callStreaming(METHOD_OBSERVE_JOBS, Empty.getDefaultInstance(), streamObserver);
            GrpcUtil.attachCancellingCallback(emitter, clientCall);
        }, Emitter.BackpressureMode.NONE);
    }

    @Override
    public Completable killJob(String jobId) {
        return toCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyStreamObserver(emitter);
            ClientCall clientCall = call(METHOD_KILL_JOB, JobId.newBuilder().setId(jobId).build(), streamObserver);
            GrpcUtil.attachCancellingCallback(emitter, clientCall);
        });
    }

    @Override
    public Observable<Task> findTask(String taskId) {
        Observable<Task> observable = Observable.create(emitter -> {
            StreamObserver<Task> streamObserver = createSimpleStreamObserver(emitter);
            ClientCall clientCall = call(METHOD_FIND_TASK, TaskId.newBuilder().setId(taskId).build(), streamObserver);
            GrpcUtil.attachCancellingCallback(emitter, clientCall);
        }, Emitter.BackpressureMode.NONE);

        observable = observable.onErrorResumeNext(e -> {
            if (e instanceof StatusRuntimeException &&
                    ((StatusRuntimeException) e).getStatus().getCode() == Status.Code.NOT_FOUND) {
                return retrieveArchivedTask(taskId);
            } else {
                return Observable.error(e);
            }
        });

        return observable.timeout(configuration.getRequestTimeout(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Observable<TaskQueryResult> findTasks(TaskQuery taskQuery) {
        Observable<TaskQueryResult> observable = Observable.create(emitter -> {
            StreamObserver<TaskQueryResult> streamObserver = createSimpleStreamObserver(emitter);
            ClientCall clientCall = call(METHOD_FIND_TASKS, taskQuery, streamObserver);
            GrpcUtil.attachCancellingCallback(emitter, clientCall);
        }, Emitter.BackpressureMode.NONE);

        observable = observable.flatMap(result -> {
            Map<String, String> filteringCriteriaMap = taskQuery.getFilteringCriteriaMap();
            Set<String> v3JobIds = StringExt.splitByComma(filteringCriteriaMap.getOrDefault("jobIds", "")).stream()
                    .filter(jobId -> !JobFunctions.isV2JobId(jobId))
                    .collect(Collectors.toSet());
            Set<String> taskStates = Sets.newHashSet(StringExt.splitByComma(filteringCriteriaMap.getOrDefault("taskStates", "")));
            if (!v3JobIds.isEmpty() && taskStates.contains(TaskState.Finished.name())) {
                return retrieveArchivedTasksForJobs(v3JobIds)
                        .map(archivedTasks -> combineTaskResults(taskQuery, result.getItemsList(), archivedTasks));
            } else {
                return Observable.just(result);
            }
        });

        return observable.timeout(configuration.getRequestTimeout(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Completable killTask(TaskKillRequest taskKillRequest) {
        return toCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyStreamObserver(emitter);
            ClientCall clientCall = call(METHOD_KILL_TASK, taskKillRequest, streamObserver);
            GrpcUtil.attachCancellingCallback(emitter, clientCall);
        });
    }

    private Observable<Job> retrieveArchivedJob(String jobId) {
        return store.retrieveArchivedJob(jobId)
                .onErrorResumeNext(e -> {
                    if (e instanceof JobStoreException) {
                        JobStoreException storeException = (JobStoreException) e;
                        if (storeException.getErrorCode().equals(JobStoreException.ErrorCode.JOB_DOES_NOT_EXIST)) {
                            return Observable.error(TitusServiceException.jobNotFound(jobId));
                        }
                    }
                    return Observable.error(TitusServiceException.unexpected("Not able to retrieve the job: %s (%s)", jobId, ExceptionExt.toMessageChain(e)));
                }).map(V3GrpcModelConverters::toGrpcJob);
    }

    private Observable<List<Task>> retrieveArchivedTasksForJobs(Set<String> jobIds) {
        return Observable.fromCallable(() -> jobIds.stream().map(store::retrieveArchivedTasksForJob).collect(Collectors.toList()))
                .flatMap(observables -> Observable.merge(observables, MAX_CONCURRENT_JOBS_TO_RETRIEVE))
                //TODO add filtering here but need to decide how to do this because most criteria is based on the job and not the task
                .map(task -> V3GrpcModelConverters.toGrpcTask(task, logStorageInfo))
                .toSortedList((first, second) -> Long.compare(first.getStatus().getTimestamp(), second.getStatus().getTimestamp()));
    }

    private TaskQueryResult combineTaskResults(TaskQuery taskQuery,
                                               List<Task> activeTasks,
                                               List<Task> archivedTasks) {
        List<Task> tasks = deDupTasks(activeTasks, archivedTasks);
        // TODO Set the cursor value after V2 engine is removed
        Page page = new Page(taskQuery.getPage().getPageNumber(), taskQuery.getPage().getPageSize(), "");

        // Selectors not supported for point queries.
        Pair<List<Task>, Pagination> paginationPair = PaginationUtil.takePage(page, tasks, task -> "");

        return TaskQueryResult.newBuilder()
                .addAllItems(paginationPair.getLeft())
                .setPagination(toGrpcPagination(paginationPair.getRight()))
                .build();
    }

    private Observable<Task> retrieveArchivedTask(String taskId) {
        return store.retrieveArchivedTask(taskId)
                .onErrorResumeNext(e -> {
                    if (e instanceof JobStoreException) {
                        JobStoreException storeException = (JobStoreException) e;
                        if (storeException.getErrorCode().equals(JobStoreException.ErrorCode.TASK_DOES_NOT_EXIST)) {
                            return Observable.error(TitusServiceException.taskNotFound(taskId));
                        }
                    }
                    return Observable.error(TitusServiceException.unexpected("Not able to retrieve the task: %s (%s)", taskId, ExceptionExt.toMessageChain(e)));
                }).map(task -> V3GrpcModelConverters.toGrpcTask(task, logStorageInfo));
    }

    private <ReqT, RespT> ClientCall call(MethodDescriptor<ReqT, RespT> methodDescriptor, ReqT request, StreamObserver<RespT> responseObserver) {
        return GrpcUtil.call(sessionContext, client, methodDescriptor, request, configuration.getRequestTimeout(), responseObserver);
    }

    private <ReqT, RespT> ClientCall callStreaming(MethodDescriptor<ReqT, RespT> methodDescriptor, ReqT request, StreamObserver<RespT> responseObserver) {
        return GrpcUtil.callStreaming(sessionContext, client, methodDescriptor, request, responseObserver);
    }

    private Completable toCompletable(Action1<Emitter<Empty>> emitter) {
        return toObservable(emitter).toCompletable();
    }

    private <T> Observable<T> toObservable(Action1<Emitter<T>> emitter) {
        return Observable.create(
                emitter,
                Emitter.BackpressureMode.NONE
        ).timeout(getRxJavaAdjustedTimeout(configuration.getRequestTimeout()), TimeUnit.MILLISECONDS);
    }

    @VisibleForTesting
    static List<Task> deDupTasks(List<Task> activeTasks, List<Task> archivedTasks) {
        Map<String, Task> archivedTasksMap = archivedTasks.stream().collect(Collectors.toMap(task -> task.getId(), Function.identity()));
        List<Task> uniqueActiveTasks = activeTasks.stream().filter(activeTask -> {
            if (archivedTasksMap.containsKey(activeTask.getId())) {
                logger.warn("Duplicate Task detected (archived) {} - (active) {}", archivedTasksMap.get(activeTask.getId()), activeTask);
                return false;
            }
            return true;
        }).collect(Collectors.toList());
        uniqueActiveTasks.addAll(archivedTasks);
        return uniqueActiveTasks;
    }
}
