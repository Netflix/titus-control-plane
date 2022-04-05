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


import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.LogStorageInfo;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.sanitizer.JobAssertions;
import com.netflix.titus.api.jobmanager.store.JobStore;
import com.netflix.titus.api.jobmanager.store.JobStoreException;
import com.netflix.titus.api.model.PageResult;
import com.netflix.titus.api.model.Pagination;
import com.netflix.titus.api.model.PaginationUtil;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.model.admission.AdmissionSanitizer;
import com.netflix.titus.common.model.admission.AdmissionValidator;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceStub;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.ObserveJobsQuery;
import com.netflix.titus.grpc.protogen.Page;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskId;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import com.netflix.titus.grpc.protogen.TaskStatus;
import com.netflix.titus.runtime.connector.GrpcRequestConfiguration;
import com.netflix.titus.runtime.endpoint.JobQueryCriteria;
import com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobQueryModelConverters;
import com.netflix.titus.runtime.jobmanager.JobManagerConfiguration;
import com.netflix.titus.runtime.jobmanager.JobManagerCursors;
import com.netflix.titus.runtime.jobmanager.gateway.GrpcJobServiceGateway;
import com.netflix.titus.runtime.jobmanager.gateway.JobServiceGateway;
import com.netflix.titus.runtime.jobmanager.gateway.JobServiceGatewayDelegate;
import com.netflix.titus.runtime.jobmanager.gateway.SanitizingJobServiceGateway;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import static com.netflix.titus.api.FeatureRolloutPlans.ENVIRONMENT_VARIABLE_NAMES_STRICT_VALIDATION_FEATURE;
import static com.netflix.titus.api.FeatureRolloutPlans.SECURITY_GROUPS_REQUIRED_FEATURE;
import static com.netflix.titus.api.jobmanager.model.job.sanitizer.JobSanitizerBuilder.JOB_STRICT_SANITIZER;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createRequestObservable;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createSimpleClientResponseObserver;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createWrappedStub;
import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobQueryModelConverters.toGrpcPagination;
import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobQueryModelConverters.toJobQueryCriteria;
import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobQueryModelConverters.toPage;

/**
 * {@link JobServiceGateway} implementation merging the active and the archived data sets with extra validation rules.
 */
@Singleton
public class GatewayJobServiceGateway extends JobServiceGatewayDelegate {

    private static Logger logger = LoggerFactory.getLogger(GatewayJobServiceGateway.class);

    private static final int MAX_CONCURRENT_JOBS_TO_RETRIEVE = 10;

    private final GrpcRequestConfiguration tunablesConfiguration;
    private final GatewayConfiguration gatewayConfiguration;
    private final JobManagementServiceStub client;
    private final LocalCacheQueryProcessor localCacheQueryProcessor;
    private final JobStore store;
    private final LogStorageInfo<com.netflix.titus.api.jobmanager.model.job.Task> logStorageInfo;
    private final TaskDataInjector taskDataInjector;
    private final NeedsMigrationQueryHandler needsMigrationQueryHandler;
    private final Clock clock;

    @Inject
    public GatewayJobServiceGateway(GrpcRequestConfiguration tunablesConfiguration,
                                    GatewayConfiguration gatewayConfiguration,
                                    JobManagerConfiguration jobManagerConfiguration,
                                    JobManagementServiceStub client,
                                    JobStore store,
                                    LogStorageInfo<com.netflix.titus.api.jobmanager.model.job.Task> logStorageInfo,
                                    TaskDataInjector taskDataInjector,
                                    NeedsMigrationQueryHandler needsMigrationQueryHandler,
                                    LocalCacheQueryProcessor localCacheQueryProcessor,
                                    @Named(JOB_STRICT_SANITIZER) EntitySanitizer entitySanitizer,
                                    DisruptionBudgetSanitizer disruptionBudgetSanitizer,
                                    @Named(SECURITY_GROUPS_REQUIRED_FEATURE) Predicate<com.netflix.titus.api.jobmanager.model.job.JobDescriptor> securityGroupsRequiredPredicate,
                                    @Named(ENVIRONMENT_VARIABLE_NAMES_STRICT_VALIDATION_FEATURE) Predicate<com.netflix.titus.api.jobmanager.model.job.JobDescriptor> environmentVariableNamesStrictValidationPredicate,
                                    JobAssertions jobAssertions,
                                    AdmissionValidator<com.netflix.titus.api.jobmanager.model.job.JobDescriptor> validator,
                                    AdmissionSanitizer<com.netflix.titus.api.jobmanager.model.job.JobDescriptor> sanitizer,
                                    TitusRuntime titusRuntime) {
        super(new SanitizingJobServiceGateway(
                new GrpcJobServiceGateway(client, tunablesConfiguration, titusRuntime),
                new ExtendedJobSanitizer(
                        jobManagerConfiguration,
                        jobAssertions,
                        entitySanitizer,
                        disruptionBudgetSanitizer,
                        securityGroupsRequiredPredicate,
                        environmentVariableNamesStrictValidationPredicate,
                        titusRuntime
                ),
                validator, sanitizer));
        this.tunablesConfiguration = tunablesConfiguration;
        this.gatewayConfiguration = gatewayConfiguration;
        this.client = client;
        this.localCacheQueryProcessor = localCacheQueryProcessor;
        this.store = store;
        this.logStorageInfo = logStorageInfo;
        this.taskDataInjector = taskDataInjector;
        this.needsMigrationQueryHandler = needsMigrationQueryHandler;
        this.clock = titusRuntime.getClock();
    }

    @Override
    public Observable<Job> findJob(String jobId, CallMetadata callMetadata) {
        TitusServiceException validationError = SanitizingJobServiceGateway.checkJobId(jobId).orElse(null);
        if (validationError != null) {
            return Observable.error(validationError);
        }

        if (localCacheQueryProcessor.canUseCache(Collections.emptyMap(), "findJob", callMetadata)) {
            return localCacheQueryProcessor.syncCache("findJob", Job.class).concatWith(
                    Observable.defer(() -> {
                        Job grpcJob = localCacheQueryProcessor.findJob(jobId).orElse(null);
                        if (grpcJob != null) {
                            return Observable.just(grpcJob);
                        }
                        return retrieveArchivedJob(jobId);
                    })
            );
        }

        Observable<Job> observable = createRequestObservable(emitter -> {
            StreamObserver<Job> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadata, tunablesConfiguration.getRequestTimeoutMs()).findJob(JobId.newBuilder().setId(jobId).build(), streamObserver);
        }, tunablesConfiguration.getRequestTimeoutMs());

        return observable.onErrorResumeNext(e -> {
            if (e instanceof StatusRuntimeException &&
                    ((StatusRuntimeException) e).getStatus().getCode() == Status.Code.NOT_FOUND) {
                return retrieveArchivedJob(jobId);
            } else {
                return Observable.error(e);
            }
        }).timeout(tunablesConfiguration.getRequestTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Observable<JobQueryResult> findJobs(JobQuery jobQuery, CallMetadata callMetadata) {
        Map<String, String> filteringCriteriaMap = jobQuery.getFilteringCriteriaMap();
        boolean needsMigrationFilter = "true".equalsIgnoreCase(filteringCriteriaMap.getOrDefault("needsMigration", "false"));

        // "needsMigration" query is served from the local job and relocation cache.
        if (needsMigrationFilter) {
            PageResult<Job> pageResult = needsMigrationQueryHandler.findJobs(GrpcJobQueryModelConverters.toJobQueryCriteria(jobQuery), toPage(jobQuery.getPage()));
            return Observable.just(JobQueryResult.newBuilder()
                    .setPagination(toGrpcPagination(pageResult.getPagination()))
                    .addAllItems(pageResult.getItems())
                    .build()
            );
        }

        if (localCacheQueryProcessor.canUseCache(jobQuery.getFilteringCriteriaMap(), "findJobs", callMetadata)) {
            return localCacheQueryProcessor.syncCache("findJobs", JobQueryResult.class).concatWith(
                    Observable.fromCallable(() -> localCacheQueryProcessor.findJobs(jobQuery))
            );
        }

        return super.findJobs(jobQuery, callMetadata);
    }

    @Override
    public Observable<Task> findTask(String taskId, CallMetadata callMetadata) {
        TitusServiceException validationError = SanitizingJobServiceGateway.checkTaskId(taskId).orElse(null);
        if (validationError != null) {
            return Observable.error(validationError);
        }

        if (localCacheQueryProcessor.canUseCache(Collections.emptyMap(), "findTask", callMetadata)) {
            return localCacheQueryProcessor.syncCache("findTask", Task.class).concatWith(
                    Observable.defer(() -> {
                        Task grpcTask = localCacheQueryProcessor.findTask(taskId).orElse(null);
                        if (grpcTask != null) {
                            return Observable.just(taskDataInjector.injectIntoTask(grpcTask));
                        }
                        return retrieveArchivedTask(taskId);
                    })
            );
        }

        Observable<Task> observable = createRequestObservable(
                emitter -> {
                    StreamObserver<Task> streamObserver = createSimpleClientResponseObserver(emitter);
                    createWrappedStub(client, callMetadata, tunablesConfiguration.getRequestTimeoutMs()).findTask(TaskId.newBuilder().setId(taskId).build(), streamObserver);
                },
                tunablesConfiguration.getRequestTimeoutMs()
        );
        observable = observable.map(taskDataInjector::injectIntoTask);

        observable = observable.onErrorResumeNext(e -> {
            if (e instanceof StatusRuntimeException &&
                    ((StatusRuntimeException) e).getStatus().getCode() == Status.Code.NOT_FOUND) {
                return retrieveArchivedTask(taskId);
            } else {
                return Observable.error(e);
            }
        });

        return observable.timeout(tunablesConfiguration.getRequestTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Observable<TaskQueryResult> findTasks(TaskQuery taskQuery, CallMetadata callMetadata) {
        Map<String, String> filteringCriteriaMap = taskQuery.getFilteringCriteriaMap();
        Set<String> v3JobIds = new HashSet<>(StringExt.splitByComma(filteringCriteriaMap.getOrDefault("jobIds", "")));
        boolean needsMigrationFilter = "true".equalsIgnoreCase(filteringCriteriaMap.getOrDefault("needsMigration", "false"));

        // "needsMigration" query is served from the local job and relocation cache.
        if (needsMigrationFilter) {
            PageResult<Task> pageResult = needsMigrationQueryHandler.findTasks(GrpcJobQueryModelConverters.toJobQueryCriteria(taskQuery), toPage(taskQuery.getPage()));
            TaskQueryResult taskQueryResult = TaskQueryResult.newBuilder()
                    .setPagination(toGrpcPagination(pageResult.getPagination()))
                    .addAllItems(taskDataInjector.injectIntoTasks(pageResult.getItems()))
                    .build();
            return Observable.just(taskQueryResult);
        }

        Set<String> taskStates = Sets.newHashSet(StringExt.splitByComma(taskQuery.getFilteringCriteriaMap().getOrDefault("taskStates", "")));

        Observable<TaskQueryResult> observable;
        if (v3JobIds.isEmpty()) {
            // Active task set only
            observable = newActiveTaskQueryAction(taskQuery, callMetadata);
        } else {
            if (!taskStates.contains(TaskState.Finished.name())) {
                // Active task set only
                observable = newActiveTaskQueryAction(taskQuery, callMetadata);
            } else {
                Page page = taskQuery.getPage();
                boolean nextPageByNumber = StringExt.isEmpty(page.getCursor()) && page.getPageNumber() > 0;

                if (nextPageByNumber) {
                    // In this case we ask for active and archived tasks using a page number > 0. Because of that
                    // we have to fetch as much tasks from master as we can. Tasks that we do not fetch, will not be
                    // visible to the client.
                    TaskQuery largePageQuery = taskQuery.toBuilder().setPage(taskQuery.getPage().toBuilder().setPageNumber(0).setPageSize(gatewayConfiguration.getMaxTaskPageSize())).build();
                    observable = newActiveTaskQueryAction(largePageQuery, callMetadata);
                } else {
                    observable = newActiveTaskQueryAction(taskQuery, callMetadata);
                }

                observable = observable.flatMap(result ->
                        retrieveArchivedTasksForJobs(v3JobIds, taskQuery).map(archivedTasks -> combineTaskResults(taskQuery, result, archivedTasks))
                );
            }
        }

        return observable.timeout(tunablesConfiguration.getRequestTimeoutMs(), TimeUnit.MILLISECONDS).map(queryResult -> taskDataInjector.injectIntoTaskQueryResult(queryResult));
    }

    @Override
    public Observable<JobChangeNotification> observeJob(String jobId, CallMetadata callMetadata) {
        TitusServiceException validationError = SanitizingJobServiceGateway.checkJobId(jobId).orElse(null);
        if (validationError != null) {
            return Observable.error(validationError);
        }

        if (localCacheQueryProcessor.canUseCache(Collections.emptyMap(), "observeJob", callMetadata)) {
            return localCacheQueryProcessor.syncCache("observeJob", JobChangeNotification.class)
                    .concatWith(localCacheQueryProcessor.observeJob(jobId))
                    .map(taskDataInjector::injectIntoTaskUpdateEvent);
        }
        return super.observeJob(jobId, callMetadata).map(taskDataInjector::injectIntoTaskUpdateEvent);
    }

    @Override
    public Observable<JobChangeNotification> observeJobs(ObserveJobsQuery query, CallMetadata callMetadata) {
        if (localCacheQueryProcessor.canUseCache(query.getFilteringCriteriaMap(), "observeJobs", callMetadata)) {
            return localCacheQueryProcessor.syncCache("observeJobs", JobChangeNotification.class)
                    .concatWith(localCacheQueryProcessor.observeJobs(query))
                    .map(taskDataInjector::injectIntoTaskUpdateEvent);
        }
        return super.observeJobs(query, callMetadata).map(taskDataInjector::injectIntoTaskUpdateEvent);
    }

    private Observable<TaskQueryResult> newActiveTaskQueryAction(TaskQuery taskQuery, CallMetadata callMetadata) {
        if (localCacheQueryProcessor.canUseCache(taskQuery.getFilteringCriteriaMap(), "findTasks", callMetadata)) {
            return localCacheQueryProcessor.syncCache("findTasks", TaskQueryResult.class).concatWith(
                    Observable.fromCallable(() -> {
                        TaskQueryResult taskQueryResult = localCacheQueryProcessor.findTasks(taskQuery);
                        return taskQueryResult.toBuilder()
                                .clearItems()
                                .addAllItems(taskDataInjector.injectIntoTasks(taskQueryResult.getItemsList()))
                                .build();
                    })
            );
        }

        return GrpcUtil.<TaskQueryResult>createRequestObservable(emitter -> {
            StreamObserver<TaskQueryResult> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadata, tunablesConfiguration.getRequestTimeoutMs()).findTasks(taskQuery, streamObserver);
        }, tunablesConfiguration.getRequestTimeoutMs()).map(taskQueryResult ->
                taskQueryResult.toBuilder()
                        .clearItems()
                        .addAllItems(taskDataInjector.injectIntoTasks(taskQueryResult.getItemsList()))
                        .build()
        );
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
                }).map(GrpcJobManagementModelConverters::toGrpcJob);
    }

    private Observable<List<Task>> retrieveArchivedTasksForJobs(Set<String> jobIds, TaskQuery taskQuery) {
        JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> taskQueryCriteria = toJobQueryCriteria(taskQuery);

        return Observable.fromCallable(() -> jobIds.stream().map(store::retrieveArchivedTasksForJob).collect(Collectors.toList()))
                .flatMap(observables -> Observable.merge(observables, MAX_CONCURRENT_JOBS_TO_RETRIEVE))
                .filter(task -> {
                    // We cannot use V3TaskQueryCriteriaEvaluator here as we do not have the job record, and requesting it
                    // is expensive. To get here, a user has to provide job id(s) for which finished tasks are requested.
                    // Because of that we do not need all the filtering criteria which mostly work at the job level to
                    // exclude jobs from the query result. Instead we implement a few that make sense for task.
                    Set<String> expectedStateReasons = taskQueryCriteria.getTaskStateReasons();
                    if (!expectedStateReasons.isEmpty() && !expectedStateReasons.contains(task.getStatus().getReasonCode())) {
                        return false;
                    }
                    if (taskQueryCriteria.isSkipSystemFailures()) {
                        if (com.netflix.titus.api.jobmanager.model.job.TaskStatus.isSystemError(task.getStatus())) {
                            return false;
                        }
                    }

                    return true;
                })
                .map(task -> {
                    com.netflix.titus.api.jobmanager.model.job.Task fixedTask = task.getStatus().getState() == TaskState.Finished
                            ? task
                            : JobFunctions.fixArchivedTaskStatus(task, clock);
                    return GrpcJobManagementModelConverters.toGrpcTask(fixedTask, logStorageInfo);
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
                    return GrpcJobManagementModelConverters.toGrpcTask(fixedTask, logStorageInfo);
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
                JobManagerCursors::newTaskCursorFrom
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
}
