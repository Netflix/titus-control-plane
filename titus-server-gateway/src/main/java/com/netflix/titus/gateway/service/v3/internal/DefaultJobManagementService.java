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


import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.validation.ConstraintViolation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import com.google.common.collect.Sets;
import com.google.protobuf.Empty;
import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.SecurityProfile;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.store.JobStore;
import com.netflix.titus.api.jobmanager.store.JobStoreException;
import com.netflix.titus.api.model.Page;
import com.netflix.titus.api.model.Pagination;
import com.netflix.titus.api.model.PaginationUtil;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.common.util.RegExpExt;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.gateway.service.v3.GrpcClientConfiguration;
import com.netflix.titus.gateway.service.v3.JobManagerConfiguration;
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
import com.netflix.titus.runtime.endpoint.common.LogStorageInfo;
import com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
import com.netflix.titus.runtime.service.JobManagementService;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;

import static com.netflix.titus.api.jobmanager.model.job.sanitizer.JobSanitizerBuilder.JOB_STRICT_SANITIZER;
import static com.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toGrpcPagination;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createRequestCompletable;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createRequestObservable;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createSimpleClientResponseObserver;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createWrappedStub;

@Singleton
public class DefaultJobManagementService implements JobManagementService {
    private static Logger logger = LoggerFactory.getLogger(DefaultAutoScalingService.class);

    private static final String TITUS_NON_COMPLIANT = "titus.noncompliant";

    private static final int MAX_CONCURRENT_JOBS_TO_RETRIEVE = 10;

    private final GrpcClientConfiguration configuration;
    private final JobManagerConfiguration jobManagerConfiguration;
    private final JobManagementServiceStub client;
    private final CallMetadataResolver callMetadataResolver;
    private final JobStore store;
    private final LogStorageInfo<com.netflix.titus.api.jobmanager.model.job.Task> logStorageInfo;
    private final EntitySanitizer entitySanitizer;
    private final Function<String, Matcher> uncompliantClientMatcher;

    @Inject
    public DefaultJobManagementService(GrpcClientConfiguration configuration,
                                       JobManagerConfiguration jobManagerConfiguration,
                                       JobManagementServiceStub client,
                                       CallMetadataResolver callMetadataResolver,
                                       JobStore store,
                                       LogStorageInfo<com.netflix.titus.api.jobmanager.model.job.Task> logStorageInfo,
                                       @Named(JOB_STRICT_SANITIZER) EntitySanitizer entitySanitizer) {
        this.configuration = configuration;
        this.jobManagerConfiguration = jobManagerConfiguration;
        this.client = client;
        this.callMetadataResolver = callMetadataResolver;
        this.store = store;
        this.logStorageInfo = logStorageInfo;
        this.entitySanitizer = entitySanitizer;
        this.uncompliantClientMatcher = RegExpExt.dynamicMatcher(
                jobManagerConfiguration::getNoncompliantClientWhiteList, "noncompliantClientWhiteList", 0, logger
        );
    }

    @Override
    public Observable<String> createJob(JobDescriptor jobDescriptor) {
        com.netflix.titus.api.jobmanager.model.job.JobDescriptor coreJobDescriptor;
        try {
            coreJobDescriptor = V3GrpcModelConverters.toCoreJobDescriptor(jobDescriptor);
        } catch (Exception e) {
            return Observable.error(TitusServiceException.invalidArgument(e));
        }
        com.netflix.titus.api.jobmanager.model.job.JobDescriptor sanitizedCoreJobDescriptor = entitySanitizer.sanitize(coreJobDescriptor).orElse(coreJobDescriptor);

        // TODO Remove this code section once all clients are compliant and they set explicitly security group(s) and IAM role.
        if (isInNonCompliantWhiteList(sanitizedCoreJobDescriptor)) {
            sanitizedCoreJobDescriptor = addMissingSecurityGroupAndIamRole(sanitizedCoreJobDescriptor);
        }
        // TODO Remove once all clients are compliant.
        sanitizedCoreJobDescriptor = checkEnvironmentViolations(sanitizedCoreJobDescriptor);

        Set<ConstraintViolation<com.netflix.titus.api.jobmanager.model.job.JobDescriptor>> violations = entitySanitizer.validate(sanitizedCoreJobDescriptor);
        if (!violations.isEmpty()) {
            return Observable.error(TitusServiceException.invalidArgument(violations));
        }

        JobDescriptor effectiveJobDescriptor = V3GrpcModelConverters.toGrpcJobDescriptor(sanitizedCoreJobDescriptor);

        return createRequestObservable(emitter -> {
            StreamObserver<JobId> streamObserver = GrpcUtil.createClientResponseObserver(
                    emitter,
                    jobId -> emitter.onNext(jobId.getId()),
                    emitter::onError,
                    emitter::onCompleted
            );
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).createJob(effectiveJobDescriptor, streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Completable updateJobCapacity(JobCapacityUpdate jobCapacityUpdate) {
        Capacity newCapacity = V3GrpcModelConverters.toCoreCapacity(jobCapacityUpdate.getCapacity());
        Set<ConstraintViolation<Capacity>> violations = entitySanitizer.validate(newCapacity);
        if (!violations.isEmpty()) {
            return Completable.error(TitusServiceException.invalidArgument(violations));
        }
        return createRequestCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).updateJobCapacity(jobCapacityUpdate, streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Completable updateJobProcesses(JobProcessesUpdate jobProcessesUpdate) {
        return createRequestCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).updateJobProcesses(jobProcessesUpdate, streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Completable updateJobStatus(JobStatusUpdate statusUpdate) {
        return createRequestCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).updateJobStatus(statusUpdate, streamObserver);
        }, configuration.getRequestTimeout());
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
    public Observable<JobQueryResult> findJobs(JobQuery jobQuery) {
        return createRequestObservable(emitter -> {
            StreamObserver<JobQueryResult> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).findJobs(jobQuery, streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Observable<JobChangeNotification> observeJob(String jobId) {
        return createRequestObservable(emitter -> {
            StreamObserver<JobChangeNotification> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver).observeJob(JobId.newBuilder().setId(jobId).build(), streamObserver);
        });
    }

    @Override
    public Observable<JobChangeNotification> observeJobs() {
        return createRequestObservable(emitter -> {
            StreamObserver<JobChangeNotification> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver).observeJobs(Empty.getDefaultInstance(), streamObserver);
        });
    }

    @Override
    public Completable killJob(String jobId) {
        return createRequestCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).killJob(JobId.newBuilder().setId(jobId).build(), streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Observable<Task> findTask(String taskId) {
        Observable<Task> observable = createRequestObservable(emitter -> {
            StreamObserver<Task> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).findTask(TaskId.newBuilder().setId(taskId).build(), streamObserver);
        }, configuration.getRequestTimeout());

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
        Observable<TaskQueryResult> observable = createRequestObservable(emitter -> {
            StreamObserver<TaskQueryResult> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).findTasks(taskQuery, streamObserver);
        }, configuration.getRequestTimeout());

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
        return createRequestCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).killTask(taskKillRequest, streamObserver);
        }, configuration.getRequestTimeout());
    }

    private boolean isInNonCompliantWhiteList(com.netflix.titus.api.jobmanager.model.job.JobDescriptor jobDescriptor) {
        com.netflix.titus.api.jobmanager.model.job.JobGroupInfo jobGroupInfo = jobDescriptor.getJobGroupInfo();
        String jobClusterId = jobDescriptor.getApplicationName() + '-' + jobGroupInfo.getStack() + '-' + jobGroupInfo.getDetail() + '-' + jobGroupInfo.getSequence();
        return uncompliantClientMatcher.apply(jobClusterId).matches();
    }

    private com.netflix.titus.api.jobmanager.model.job.JobDescriptor addMissingSecurityGroupAndIamRole(com.netflix.titus.api.jobmanager.model.job.JobDescriptor<?> jobDescriptor) {
        SecurityProfile securityProfile = jobDescriptor.getContainer().getSecurityProfile();
        if (!securityProfile.getSecurityGroups().isEmpty() && !securityProfile.getIamRole().isEmpty()) {
            return jobDescriptor;
        }
        SecurityProfile.Builder builder = securityProfile.toBuilder();
        String nonCompliant = null;
        if (securityProfile.getSecurityGroups().isEmpty()) {
            builder.withSecurityGroups(jobManagerConfiguration.getDefaultSecurityGroups());
            nonCompliant = "noSecurityGroups";
        }
        if (securityProfile.getIamRole().isEmpty()) {
            builder.withIamRole(jobManagerConfiguration.getDefaultIamRole());
            nonCompliant = nonCompliant == null ? "noIamRole" : nonCompliant + ",noIamRole";
        }
        com.netflix.titus.api.jobmanager.model.job.JobDescriptor<?> sanitizedJobDescriptor = jobDescriptor.toBuilder()
                .withContainer(jobDescriptor.getContainer().toBuilder()
                        .withSecurityProfile(builder.build()).build()
                ).build();
        return markNonCompliant(sanitizedJobDescriptor, nonCompliant);
    }

    private com.netflix.titus.api.jobmanager.model.job.JobDescriptor checkEnvironmentViolations(com.netflix.titus.api.jobmanager.model.job.JobDescriptor jobDescriptor) {
        Map<String, String> env = jobDescriptor.getContainer().getEnv();
        if (CollectionsExt.isNullOrEmpty(env)) {
            return jobDescriptor;
        }

        boolean allAsciiCharacters = env.entrySet().stream().allMatch(entry -> isAscii(entry.getKey()) && isAscii(entry.getValue()));
        boolean noDotInKeyName = env.keySet().stream().allMatch(key -> key == null || !key.contains("."));

        if (allAsciiCharacters && noDotInKeyName) {
            return jobDescriptor;
        }

        String nonCompliant = allAsciiCharacters
                ? "environmentVariableNameWithDot"
                : (noDotInKeyName ? "nonAsciiCharactersInEnvironmentVariable" : "environmentVariableNameWithDot,nonAsciiCharactersInEnvironmentVariable");

        return markNonCompliant(jobDescriptor, nonCompliant);
    }

    private com.netflix.titus.api.jobmanager.model.job.JobDescriptor markNonCompliant(com.netflix.titus.api.jobmanager.model.job.JobDescriptor<?> jobDescriptor, String nonCompliant) {
        Map<String, String> attributes = jobDescriptor.getAttributes();
        String previousNonCompliant = attributes.get(TITUS_NON_COMPLIANT);
        String newNonCompliant = previousNonCompliant == null ? nonCompliant : previousNonCompliant + ',' + nonCompliant;
        return jobDescriptor.toBuilder()
                .withAttributes(CollectionsExt.copyAndAdd(jobDescriptor.getAttributes(), TITUS_NON_COMPLIANT, newNonCompliant))
                .build();
    }

    private boolean isAscii(String value) {
        return value == null || CharMatcher.ascii().matchesAllOf(value);
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

        // Cursors not supported for point queries.
        Pair<List<Task>, Pagination> paginationPair = PaginationUtil.takePageWithoutCursor(page, tasks, task -> "");

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