/*
 * Copyright 2021 Netflix, Inc.
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.LogStorageInfo;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import com.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import com.netflix.titus.api.model.Page;
import com.netflix.titus.api.model.PageResult;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.RegExpExt;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.gateway.MetricConstants;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.ObserveJobsQuery;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import com.netflix.titus.grpc.protogen.TaskStatus;
import com.netflix.titus.runtime.connector.jobmanager.JobDataReplicator;
import com.netflix.titus.runtime.connector.jobmanager.JobSnapshot;
import com.netflix.titus.runtime.endpoint.JobQueryCriteria;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobQueryModelConverters;
import com.netflix.titus.runtime.endpoint.v3.grpc.query.V3JobQueryCriteriaEvaluator;
import com.netflix.titus.runtime.endpoint.v3.grpc.query.V3TaskQueryCriteriaEvaluator;
import com.netflix.titus.runtime.jobmanager.JobManagerCursors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import rx.Observable;

import static com.netflix.titus.runtime.endpoint.common.grpc.CommonRuntimeGrpcModelConverters.toGrpcPagination;
import static com.netflix.titus.runtime.endpoint.common.grpc.CommonRuntimeGrpcModelConverters.toPage;
import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobQueryModelConverters.toJobQueryCriteria;

@Singleton
public class LocalCacheQueryProcessor {

    private static final Logger logger = LoggerFactory.getLogger(LocalCacheQueryProcessor.class);

    public static final String PARAMETER_USE_CACHE = "useCache";

    private static final JobChangeNotification SNAPSHOT_END_MARKER = JobChangeNotification.newBuilder()
            .setSnapshotEnd(JobChangeNotification.SnapshotEnd.newBuilder())
            .build();

    private static final String METRIC_ROOT = MetricConstants.METRIC_ROOT + "localCacheQueryProcessor.";
    private static final String METRIC_CACHE_USE_ALLOWED = METRIC_ROOT + "cacheUseAllowed";

    private final GatewayConfiguration configuration;
    private final JobDataReplicator jobDataReplicator;
    private final LogStorageInfo<com.netflix.titus.api.jobmanager.model.job.Task> logStorageInfo;
    private final TitusRuntime titusRuntime;

    private final Function<String, Matcher> callerIdMatcher;

    @Inject
    public LocalCacheQueryProcessor(GatewayConfiguration configuration,
                                    JobDataReplicator jobDataReplicator,
                                    LogStorageInfo<com.netflix.titus.api.jobmanager.model.job.Task> logStorageInfo,
                                    TitusRuntime titusRuntime) {
        this.configuration = configuration;
        this.jobDataReplicator = jobDataReplicator;
        this.logStorageInfo = logStorageInfo;
        this.titusRuntime = titusRuntime;
        this.callerIdMatcher = RegExpExt.dynamicMatcher(configuration::getQueryFromCacheCallerId,
                "titusGateway.queryFromCacheCallerId", Pattern.DOTALL, logger);
    }

    public boolean canUseCache(Map<String, String> queryParameters,
                               String endpoint,
                               CallMetadata callMetadata) {
        boolean allow;
        String reason;

        String callerId = CollectionsExt.isNullOrEmpty(callMetadata.getCallers())
                ? "unknown"
                : callMetadata.getCallers().get(0).getId();

        if (jobDataReplicator.getStalenessMs() > configuration.getMaxAcceptableCacheStaleness()) {
            allow = false;
            reason = "stale";
        } else {
            // Check if cache explicitly requested.
            if ("true".equalsIgnoreCase(queryParameters.getOrDefault(PARAMETER_USE_CACHE, "false"))) {
                allow = true;
                reason = "requestedDirectly";
            } else if (callerId.equals("unknown")) {
                allow = false;
                reason = "callerNotSet";
            } else {
                allow = callerIdMatcher.apply(callerId).matches();
                reason = allow ? "configurationMatch" : "callerNotMatching";
            }
        }

        titusRuntime.getRegistry()
                .counter(METRIC_CACHE_USE_ALLOWED,
                        "endpoint", endpoint,
                        "callerId", callerId,
                        "allowed", Boolean.toString(allow),
                        "reason", reason
                )
                .increment();

        return allow;
    }

    public JobQueryResult findJobs(JobQuery jobQuery) {
        JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> queryCriteria = GrpcJobQueryModelConverters.toJobQueryCriteria(jobQuery);
        Page page = toPage(jobQuery.getPage());

        List<Job> matchingJobs = findMatchingJob(queryCriteria);
        PageResult<Job> pageResult = JobManagerCursors.newCoreJobPaginationEvaluator().takePage(page, matchingJobs);

        List<com.netflix.titus.grpc.protogen.Job> grpcJob = pageResult.getItems().stream()
                .map(GrpcJobManagementModelConverters::toGrpcJob)
                .collect(Collectors.toList());

        return JobQueryResult.newBuilder()
                .setPagination(toGrpcPagination(pageResult.getPagination()))
                .addAllItems(grpcJob)
                .build();
    }

    public TaskQueryResult findTasks(TaskQuery taskQuery) {
        JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> queryCriteria = GrpcJobQueryModelConverters.toJobQueryCriteria(taskQuery);
        Page page = toPage(taskQuery.getPage());

        List<com.netflix.titus.api.jobmanager.model.job.Task> matchingTasks = findMatchingTasks(queryCriteria);
        PageResult<com.netflix.titus.api.jobmanager.model.job.Task> pageResult = JobManagerCursors.newCoreTaskPaginationEvaluator().takePage(page, matchingTasks);

        List<Task> grpcTasks = pageResult.getItems().stream()
                .map(task -> GrpcJobManagementModelConverters.toGrpcTask(task, logStorageInfo))
                .collect(Collectors.toList());

        return TaskQueryResult.newBuilder()
                .setPagination(toGrpcPagination(pageResult.getPagination()))
                .addAllItems(grpcTasks)
                .build();
    }

    public Observable<JobChangeNotification> observeJobs(ObserveJobsQuery query) {
        JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> criteria = toJobQueryCriteria(query);
        V3JobQueryCriteriaEvaluator jobsPredicate = new V3JobQueryCriteriaEvaluator(criteria, titusRuntime);
        V3TaskQueryCriteriaEvaluator tasksPredicate = new V3TaskQueryCriteriaEvaluator(criteria, titusRuntime);

        Flux<JobChangeNotification> eventStream = Flux.defer(() -> {
            AtomicBoolean first = new AtomicBoolean(true);
            return jobDataReplicator.events().flatMap(event -> {
                long now = titusRuntime.getClock().wallTime();
                Optional<JobChangeNotification> grpcEvent = toObserveJobsEvent(event.getLeft(), event.getRight(), now, jobsPredicate, tasksPredicate);

                // On first event emit full snapshot first
                if (first.getAndSet(false)) {
                    List<JobChangeNotification> snapshotEvents = buildSnapshot(event.getLeft(), now, jobsPredicate, tasksPredicate);
                    grpcEvent.ifPresent(snapshotEvents::add);
                    return Flux.fromIterable(snapshotEvents);
                }
                return grpcEvent.map(Flux::just).orElseGet(Flux::empty);
            });
        });
        return ReactorExt.toObservable(eventStream);
    }

    private List<com.netflix.titus.api.jobmanager.model.job.Job> findMatchingJob(JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> queryCriteria) {
        JobSnapshot jobSnapshot = jobDataReplicator.getCurrent();
        Map<String, Job<?>> jobsById = jobSnapshot.getJobMap();

        V3JobQueryCriteriaEvaluator queryFilter = new V3JobQueryCriteriaEvaluator(queryCriteria, titusRuntime);

        List<com.netflix.titus.api.jobmanager.model.job.Job> matchingJobs = new ArrayList<>();
        jobsById.forEach((jobId, job) -> {
            List<com.netflix.titus.api.jobmanager.model.job.Task> tasks = jobSnapshot.getTasks(jobId);
            Pair<Job<?>, List<com.netflix.titus.api.jobmanager.model.job.Task>> jobTaskPair = Pair.of(job, tasks);
            if (queryFilter.test(jobTaskPair)) {
                matchingJobs.add(job);
            }
        });

        return matchingJobs;
    }

    private List<com.netflix.titus.api.jobmanager.model.job.Task> findMatchingTasks(JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> queryCriteria) {
        JobSnapshot jobSnapshot = jobDataReplicator.getCurrent();
        Map<String, Job<?>> jobsById = jobSnapshot.getJobMap();

        V3TaskQueryCriteriaEvaluator queryFilter = new V3TaskQueryCriteriaEvaluator(queryCriteria, titusRuntime);

        List<com.netflix.titus.api.jobmanager.model.job.Task> matchingTasks = new ArrayList<>();
        jobsById.forEach((jobId, job) -> {
            List<com.netflix.titus.api.jobmanager.model.job.Task> tasks = jobSnapshot.getTasks(jobId);
            if (!CollectionsExt.isNullOrEmpty(tasks)) {
                tasks.forEach(task -> {
                    Pair<Job<?>, com.netflix.titus.api.jobmanager.model.job.Task> jobTaskPair = Pair.of(job, task);
                    if (queryFilter.test(jobTaskPair)) {
                        matchingTasks.add(task);
                    }
                });
            }
        });

        return matchingTasks;
    }

    private List<JobChangeNotification> buildSnapshot(JobSnapshot snapshot,
                                                      long now,
                                                      V3JobQueryCriteriaEvaluator jobsPredicate,
                                                      V3TaskQueryCriteriaEvaluator tasksPredicate) {
        List<JobChangeNotification> result = new ArrayList<>();
        Map<String, Job<?>> allJobsMap = snapshot.getJobMap();

        allJobsMap.forEach((jobId, job) -> {
            List<com.netflix.titus.api.jobmanager.model.job.Task> tasks = snapshot.getTasks(jobId);
            if (jobsPredicate.test(Pair.of(job, tasks))) {
                result.add(toGrpcJobEvent(job, now));
            }
        });

        snapshot.getTaskMap().forEach((taskId, task) -> {
            Job<?> job = allJobsMap.get(task.getJobId());
            if (job != null && tasksPredicate.test(Pair.of(job, task))) {
                result.add(toGrpcTaskEvent(task, false, now));
            }
        });

        result.add(SNAPSHOT_END_MARKER);

        return result;
    }

    private Optional<JobChangeNotification> toObserveJobsEvent(JobSnapshot snapshot,
                                                               JobManagerEvent<?> event,
                                                               long now,
                                                               V3JobQueryCriteriaEvaluator jobsPredicate,
                                                               V3TaskQueryCriteriaEvaluator tasksPredicate) {
        if (event instanceof JobUpdateEvent) {
            JobUpdateEvent jobUpdateEvent = (JobUpdateEvent) event;
            Job<?> job = jobUpdateEvent.getCurrent();
            List<com.netflix.titus.api.jobmanager.model.job.Task> tasks = snapshot.getTasks(job.getId());
            return jobsPredicate.test(Pair.of(job, tasks)) ? Optional.of(toGrpcJobEvent(job, now)) : Optional.empty();
        }

        if (event instanceof TaskUpdateEvent) {
            TaskUpdateEvent taskUpdateEvent = (TaskUpdateEvent) event;
            Job<?> job = taskUpdateEvent.getCurrentJob();
            com.netflix.titus.api.jobmanager.model.job.Task task = taskUpdateEvent.getCurrentTask();
            return tasksPredicate.test(Pair.of(job, task))
                    ? Optional.of(toGrpcTaskEvent(task, taskUpdateEvent.isMovedFromAnotherJob(), now))
                    : Optional.empty();
        }
        return Optional.empty();
    }

    private JobChangeNotification toGrpcJobEvent(Job<?> job, long now) {
        return JobChangeNotification.newBuilder()
                .setJobUpdate(JobChangeNotification.JobUpdate.newBuilder()
                        .setJob(GrpcJobManagementModelConverters.toGrpcJob(job))
                )
                .setTimestamp(now)
                .build();
    }

    private JobChangeNotification toGrpcTaskEvent(com.netflix.titus.api.jobmanager.model.job.Task task,
                                                  boolean movedFromAnotherJob,
                                                  long now) {
        return JobChangeNotification.newBuilder()
                .setTaskUpdate(
                        JobChangeNotification.TaskUpdate.newBuilder()
                                .setTask(GrpcJobManagementModelConverters.toGrpcTask(task, logStorageInfo))
                                .setMovedFromAnotherJob(movedFromAnotherJob)
                )
                .setTimestamp(now)
                .build();
    }
}
