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

package com.netflix.titus.gateway.service.v3.internal;


import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.store.JobStore;
import com.netflix.titus.api.jobmanager.store.JobStoreException;
import com.netflix.titus.api.model.Pagination;
import com.netflix.titus.api.model.PaginationUtil;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.common.model.validator.EntityValidator;
import com.netflix.titus.common.model.validator.ValidationError;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.gateway.service.v3.JobManagerConfiguration;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceStub;
import com.netflix.titus.grpc.protogen.Page;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskId;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import com.netflix.titus.runtime.connector.GrpcClientConfiguration;
import com.netflix.titus.runtime.connector.jobmanager.JobManagementClient;
import com.netflix.titus.runtime.connector.jobmanager.client.GrpcJobManagementClient;
import com.netflix.titus.runtime.connector.jobmanager.client.JobManagementClientDelegate;
import com.netflix.titus.runtime.endpoint.common.LogStorageInfo;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
import com.netflix.titus.runtime.jobmanager.JobManagerCursors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import static com.netflix.titus.api.jobmanager.model.job.sanitizer.JobSanitizerBuilder.JOB_STRICT_SANITIZER;
import static com.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toGrpcPagination;
import static com.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toPage;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createRequestObservable;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createSimpleClientResponseObserver;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createWrappedStub;

/**
 * {@link JobManagementClient} implementation merging the active and the archived data sets with extra validation rules.
 */
@Singleton
public class GatewayJobManagementClient extends JobManagementClientDelegate {

    private static Logger logger = LoggerFactory.getLogger(GatewayJobManagementClient.class);

    private static final int MAX_CONCURRENT_JOBS_TO_RETRIEVE = 10;

    private final GrpcClientConfiguration configuration;
    private final JobManagementServiceStub client;
    private final CallMetadataResolver callMetadataResolver;
    private final JobStore store;
    private final LogStorageInfo<com.netflix.titus.api.jobmanager.model.job.Task> logStorageInfo;
    private final TaskRelocationDataInjector taskRelocationDataInjector;
    private final EntityValidator<com.netflix.titus.api.jobmanager.model.job.JobDescriptor> validator;
    private final Registry spectatorRegistry;
    private final Clock clock;

    @Inject
    public GatewayJobManagementClient(GrpcClientConfiguration configuration,
                                      JobManagerConfiguration jobManagerConfiguration,
                                      JobManagementServiceStub client,
                                      CallMetadataResolver callMetadataResolver,
                                      JobStore store,
                                      LogStorageInfo<com.netflix.titus.api.jobmanager.model.job.Task> logStorageInfo,
                                      TaskRelocationDataInjector taskRelocationDataInjector,
                                      @Named(JOB_STRICT_SANITIZER) EntitySanitizer entitySanitizer,
                                      EntityValidator<com.netflix.titus.api.jobmanager.model.job.JobDescriptor> validator,
                                      TitusRuntime titusRuntime) {
        super(new GrpcJobManagementClient(
                client,
                callMetadataResolver,
                new ExtendedJobSanitizer(jobManagerConfiguration, entitySanitizer),
                configuration
        ));
        this.configuration = configuration;
        this.client = client;
        this.callMetadataResolver = callMetadataResolver;
        this.store = store;
        this.logStorageInfo = logStorageInfo;
        this.taskRelocationDataInjector = taskRelocationDataInjector;
        this.validator = validator;
        this.spectatorRegistry = titusRuntime.getRegistry();
        this.clock = titusRuntime.getClock();
    }

    @Override
    public Observable<String> createJob(JobDescriptor jobDescriptor) {
        com.netflix.titus.api.jobmanager.model.job.JobDescriptor coreJobDescriptor;
        try {
            coreJobDescriptor = V3GrpcModelConverters.toCoreJobDescriptor(jobDescriptor);
        } catch (Exception e) {
            return Observable.error(TitusServiceException.invalidArgument(e));
        }

        Observable<com.netflix.titus.api.jobmanager.model.job.JobDescriptor> sanitizedCoreJobDescriptorObs =
                ReactorExt.toObservable(validator.sanitize(coreJobDescriptor))
                        .onErrorResumeNext(throwable -> Observable.error(TitusServiceException.invalidArgument(throwable)))
                        .flatMap(sanitizedCoreJobDescriptor -> ReactorExt.toObservable(validator.validate(sanitizedCoreJobDescriptor))
                                .flatMap(errors -> {
                                    // Report metrics on all errors
                                    reportErrorMetrics(errors, sanitizedCoreJobDescriptor);

                                    // Only emit an error on HARD validation errors
                                    errors = errors.stream().filter(ValidationError::isHard).collect(Collectors.toSet());

                                    if (!errors.isEmpty()) {
                                        return Observable.error(TitusServiceException.invalidJob(errors));
                                    } else {
                                        return Observable.just(sanitizedCoreJobDescriptor);
                                    }
                                })
                        );

        return sanitizedCoreJobDescriptorObs.flatMap(scjd -> super.createJob(V3GrpcModelConverters.toGrpcJobDescriptor(scjd)));
    }

    @Override
    public Observable<Job> findJob(String jobId) {
        Observable<Job> observable = createRequestObservable(emitter -> {
            StreamObserver<Job> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).findJob(JobId.newBuilder().setId(jobId).build(), streamObserver);
        }, configuration.getRequestTimeout());

        return observable.onErrorResumeNext(e -> {
            if (e instanceof StatusRuntimeException &&
                    ((StatusRuntimeException) e).getStatus().getCode() == Status.Code.NOT_FOUND) {
                return retrieveArchivedJob(jobId);
            } else {
                return Observable.error(e);
            }
        }).timeout(configuration.getRequestTimeout(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Observable<Task> findTask(String taskId) {
        Observable<Task> observable = createRequestObservable(
                emitter -> {
                    StreamObserver<Task> streamObserver = createSimpleClientResponseObserver(emitter);
                    createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).findTask(TaskId.newBuilder().setId(taskId).build(), streamObserver);
                },
                configuration.getRequestTimeout()
        );
        observable = taskRelocationDataInjector.injectIntoTask(taskId, observable);

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
        Map<String, String> filteringCriteriaMap = taskQuery.getFilteringCriteriaMap();
        Set<String> v3JobIds = new HashSet<>(StringExt.splitByComma(filteringCriteriaMap.getOrDefault("jobIds", "")));

        Observable<TaskQueryResult> observable;
        if (v3JobIds.isEmpty()) {
            // Active task set only
            observable = newActiveTaskQueryAction(taskQuery);
        } else {
            Set<String> taskStates = Sets.newHashSet(StringExt.splitByComma(taskQuery.getFilteringCriteriaMap().getOrDefault("taskStates", "")));

            if (!taskStates.contains(TaskState.Finished.name())) {
                // Active task set only
                observable = newActiveTaskQueryAction(taskQuery);
            } else {
                Page page = taskQuery.getPage();
                boolean nextPageByNumber = StringExt.isEmpty(page.getCursor()) && page.getPageNumber() > 0;

                if (nextPageByNumber) {
                    // In this case we ask for active and archived tasks using a page number > 0. Because of that
                    // we have to fetch as much tasks from master as we can. Tasks that we do not fetch, will not be
                    // visible to the client.
                    TaskQuery largePageQuery = taskQuery.toBuilder().setPage(taskQuery.getPage().toBuilder().setPageNumber(0).setPageSize(configuration.getMaxTaskPageSize())).build();
                    observable = newActiveTaskQueryAction(largePageQuery);
                } else {
                    observable = newActiveTaskQueryAction(taskQuery);
                }

                observable = observable.flatMap(result ->
                        retrieveArchivedTasksForJobs(v3JobIds).map(archivedTasks -> combineTaskResults(taskQuery, result, archivedTasks))
                );
            }
        }

        return taskRelocationDataInjector.injectIntoTaskQueryResult(observable.timeout(configuration.getRequestTimeout(), TimeUnit.MILLISECONDS));
    }

    private Observable<TaskQueryResult> newActiveTaskQueryAction(TaskQuery taskQuery) {
        return createRequestObservable(emitter -> {
            StreamObserver<TaskQueryResult> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).findTasks(taskQuery, streamObserver);
        }, configuration.getRequestTimeout());
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
                .map(task -> {
                    com.netflix.titus.api.jobmanager.model.job.Task fixedTask = task.getStatus().getState() == TaskState.Finished
                            ? task
                            : JobFunctions.fixArchivedTaskStatus(task, clock);
                    return V3GrpcModelConverters.toGrpcTask(fixedTask, logStorageInfo);
                })
                .toSortedList((first, second) -> Long.compare(first.getStatus().getTimestamp(), second.getStatus().getTimestamp()));
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
                })
                .map(task -> {
                    com.netflix.titus.api.jobmanager.model.job.Task fixedTask = task.getStatus().getState() == TaskState.Finished
                            ? task
                            : JobFunctions.fixArchivedTaskStatus(task, clock);
                    return V3GrpcModelConverters.toGrpcTask(fixedTask, logStorageInfo);
                });
    }

    @VisibleForTesting
    static TaskQueryResult combineTaskResults(TaskQuery taskQuery,
                                              TaskQueryResult activeTasksResult,
                                              List<Task> archivedTasks) {
        List<Task> tasks = deDupTasks(activeTasksResult.getItemsList(), archivedTasks);

        Pair<List<Task>, Pagination> paginationPair = PaginationUtil.takePageWithCursor(
                toPage(taskQuery.getPage()),
                tasks,
                JobManagerCursors.taskCursorOrderComparator(),
                JobManagerCursors::taskIndexOf,
                JobManagerCursors::newCursorFrom
        );

        // Fix pagination result, as the total items count does not include all active tasks.
        // The total could be larger than the actual number of tasks, as we are not filtering duplicates.
        // This could be fixed in the future, when the gateway stores all active tasks in a local cache.
        int allTasksCount = activeTasksResult.getPagination().getTotalItems() + archivedTasks.size();
        Pair<List<Task>, Pagination> fixedPaginationPair = paginationPair.mapRight(p -> p.toBuilder()
                .withTotalItems(allTasksCount)
                .withTotalPages(PaginationUtil.numberOfPages(toPage(taskQuery.getPage()), allTasksCount))
                .build()
        );

        return TaskQueryResult.newBuilder()
                .addAllItems(fixedPaginationPair.getLeft())
                .setPagination(toGrpcPagination(fixedPaginationPair.getRight()))
                .build();
    }

    /**
     * It is ok to find the same task in the active and the archived data set. This may happen as the active and the archive
     * queries are run one after the other. In such case we know that the archive task is the latest copy, and should be
     * returned to the client.
     */
    @VisibleForTesting
    static List<Task> deDupTasks(List<Task> activeTasks, List<Task> archivedTasks) {
        Map<String, Task> archivedTasksMap = archivedTasks.stream().collect(Collectors.toMap(Task::getId, Function.identity()));
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

    private void reportErrorMetrics(Set<ValidationError> errors, com.netflix.titus.api.jobmanager.model.job.JobDescriptor jobDescriptor) {
        errors.forEach(error ->
                spectatorRegistry.counter(
                        error.getField(),
                        "type", error.getType().name(),
                        "description", error.getDescription(),
                        "application", jobDescriptor.getApplicationName())
                        .increment());
    }
}