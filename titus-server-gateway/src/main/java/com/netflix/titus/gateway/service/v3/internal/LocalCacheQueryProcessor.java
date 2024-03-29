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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.LogStorageInfo;
import com.netflix.titus.api.jobmanager.model.job.event.JobKeepAliveEvent;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import com.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import com.netflix.titus.api.model.Page;
import com.netflix.titus.api.model.PageResult;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.ProtobufExt;
import com.netflix.titus.common.util.RegExpExt;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.spectator.MetricSelector;
import com.netflix.titus.common.util.spectator.SpectatorExt;
import com.netflix.titus.common.util.spectator.ValueRangeCounter;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.gateway.MetricConstants;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.JobStatus;
import com.netflix.titus.grpc.protogen.ObserveJobsQuery;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import com.netflix.titus.grpc.protogen.TaskStatus;
import com.netflix.titus.runtime.connector.jobmanager.JobDataReplicator;
import com.netflix.titus.runtime.connector.jobmanager.snapshot.JobSnapshot;
import com.netflix.titus.runtime.endpoint.JobQueryCriteria;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobQueryModelConverters;
import com.netflix.titus.runtime.endpoint.v3.grpc.query.V3JobQueryCriteriaEvaluator;
import com.netflix.titus.runtime.endpoint.v3.grpc.query.V3TaskQueryCriteriaEvaluator;
import com.netflix.titus.runtime.jobmanager.JobManagerCursors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import rx.Observable;

import static com.netflix.titus.runtime.endpoint.common.grpc.CommonRuntimeGrpcModelConverters.toGrpcPagination;
import static com.netflix.titus.runtime.endpoint.common.grpc.CommonRuntimeGrpcModelConverters.toPage;
import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobQueryModelConverters.toJobQueryCriteria;
import static com.netflix.titus.runtime.jobmanager.gateway.JobServiceGateway.JOB_MINIMUM_FIELD_SET;
import static com.netflix.titus.runtime.jobmanager.gateway.JobServiceGateway.TASK_MINIMUM_FIELD_SET;

@Singleton
public class LocalCacheQueryProcessor {

    private static final Logger logger = LoggerFactory.getLogger(LocalCacheQueryProcessor.class);

    public static final String PARAMETER_USE_CACHE = "useCache";

    private static final JobChangeNotification SNAPSHOT_END_MARKER = JobChangeNotification.newBuilder()
            .setSnapshotEnd(JobChangeNotification.SnapshotEnd.newBuilder())
            .build();

    private static final String METRIC_ROOT = MetricConstants.METRIC_ROOT + "localCacheQueryProcessor.";
    private static final String METRIC_CACHE_USE_ALLOWED = METRIC_ROOT + "cacheUseAllowed";
    private static final long[] LEVELS = new long[]{1, 2, 5, 10, 20, 30, 40, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 30_000};

    private final GatewayConfiguration configuration;
    private final JobDataReplicator jobDataReplicator;
    private final LogStorageInfo<com.netflix.titus.api.jobmanager.model.job.Task> logStorageInfo;
    private final TitusRuntime titusRuntime;

    private final Function<String, Matcher> callerIdMatcher;
    private final MetricSelector<ValueRangeCounter> syncDelayMetric;
    private final Counter rejectedByStalenessTooHighMetric;

    private final Scheduler scheduler;

    @Inject
    public LocalCacheQueryProcessor(GatewayConfiguration configuration,
                                    JobDataReplicator jobDataReplicator,
                                    LogStorageInfo<com.netflix.titus.api.jobmanager.model.job.Task> logStorageInfo,
                                    TitusRuntime titusRuntime) {
        this(configuration,
                jobDataReplicator,
                logStorageInfo,
                Schedulers.newBoundedElastic(
                        configuration.getLocalCacheSchedulerThreadPoolSize(),
                        Integer.MAX_VALUE,
                        LocalCacheQueryProcessor.class.getSimpleName() + "-event-processor"
                ),
                titusRuntime
        );
    }

    public LocalCacheQueryProcessor(GatewayConfiguration configuration,
                                    JobDataReplicator jobDataReplicator,
                                    LogStorageInfo<com.netflix.titus.api.jobmanager.model.job.Task> logStorageInfo,
                                    Scheduler scheduler,
                                    TitusRuntime titusRuntime) {
        this.configuration = configuration;
        this.jobDataReplicator = jobDataReplicator;
        this.logStorageInfo = logStorageInfo;
        this.titusRuntime = titusRuntime;
        this.scheduler = scheduler;
        this.callerIdMatcher = RegExpExt.dynamicMatcher(configuration::getQueryFromCacheCallerId,
                "titusGateway.queryFromCacheCallerId", Pattern.DOTALL, logger);

        Registry registry = titusRuntime.getRegistry();
        this.syncDelayMetric = SpectatorExt.newValueRangeCounter(
                registry.createId(METRIC_ROOT + "syncDelay"), new String[]{"endpoint"}, LEVELS, registry
        );
        this.rejectedByStalenessTooHighMetric = registry.counter(METRIC_ROOT + "rejectedByStalenessTooHigh");
    }

    @PreDestroy
    public void shutdown() {
        scheduler.dispose();
    }

    public boolean canUseCache(Map<String, String> queryParameters,
                               String endpoint,
                               CallMetadata callMetadata) {
        boolean allow;
        String reason;

        String callerId = CollectionsExt.isNullOrEmpty(callMetadata.getCallers())
                ? "unknown"
                : callMetadata.getCallers().get(0).getId();

        if (jobDataReplicator.getStalenessMs() > configuration.getMaxAcceptableCacheStalenessMs()) {
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

    public Optional<com.netflix.titus.grpc.protogen.Job> findJob(String jobId) {
        return jobDataReplicator.getCurrent().findJob(jobId).map(GrpcJobManagementModelConverters::toGrpcJob);
    }

    public JobQueryResult findJobs(JobQuery jobQuery) {
        JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> queryCriteria = GrpcJobQueryModelConverters.toJobQueryCriteria(jobQuery);
        Page page = toPage(jobQuery.getPage());

        List<Job> matchingJobs = findMatchingJob(queryCriteria);
        PageResult<Job> pageResult = JobManagerCursors.newCoreJobPaginationEvaluator().takePage(page, matchingJobs);

        Set<String> fields = newFieldsFilter(jobQuery.getFieldsList(), JOB_MINIMUM_FIELD_SET);
        List<com.netflix.titus.grpc.protogen.Job> grpcJob = pageResult.getItems().stream()
                .map(coreJob -> {
                    com.netflix.titus.grpc.protogen.Job job = GrpcJobManagementModelConverters.toGrpcJob(coreJob);
                    if (!fields.isEmpty()) {
                        job = ProtobufExt.copy(job, fields);
                    }
                    return job;
                })
                .collect(Collectors.toList());

        return JobQueryResult.newBuilder()
                .setPagination(toGrpcPagination(pageResult.getPagination()))
                .addAllItems(grpcJob)
                .build();
    }

    public Optional<Task> findTask(String taskId) {
        return jobDataReplicator.getCurrent()
                .findTaskById(taskId)
                .map(jobTaskPair -> GrpcJobManagementModelConverters.toGrpcTask(jobTaskPair.getRight(), logStorageInfo));
    }

    public TaskQueryResult findTasks(TaskQuery taskQuery) {
        JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> queryCriteria = GrpcJobQueryModelConverters.toJobQueryCriteria(taskQuery);
        Page page = toPage(taskQuery.getPage());

        List<com.netflix.titus.api.jobmanager.model.job.Task> matchingTasks = findMatchingTasks(queryCriteria);
        PageResult<com.netflix.titus.api.jobmanager.model.job.Task> pageResult = JobManagerCursors.newCoreTaskPaginationEvaluator().takePage(page, matchingTasks);

        Set<String> fields = newFieldsFilter(taskQuery.getFieldsList(), TASK_MINIMUM_FIELD_SET);
        List<Task> grpcTasks = pageResult.getItems().stream()
                .map(task -> {
                    Task grpcTask = GrpcJobManagementModelConverters.toGrpcTask(task, logStorageInfo);
                    if (!fields.isEmpty()) {
                        grpcTask = ProtobufExt.copy(grpcTask, fields);
                    }
                    return grpcTask;
                })
                .collect(Collectors.toList());

        return TaskQueryResult.newBuilder()
                .setPagination(toGrpcPagination(pageResult.getPagination()))
                .addAllItems(grpcTasks)
                .build();
    }

    public Observable<JobChangeNotification> observeJob(String jobId) {
        ObserveJobsQuery query = ObserveJobsQuery.newBuilder().putFilteringCriteria("jobIds", jobId).build();
        return observeJobs(query).takeUntil(this::isJobFinishedEvent);
    }

    /**
     * Job finished event is the last one that is emitted for every completed job.
     */
    private boolean isJobFinishedEvent(JobChangeNotification event) {
        return event.getNotificationCase() == JobChangeNotification.NotificationCase.JOBUPDATE &&
                event.getJobUpdate().getJob().getStatus().getState() == JobStatus.JobState.Finished;
    }

    public Observable<JobChangeNotification> observeJobs(ObserveJobsQuery query) {
        JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> criteria = toJobQueryCriteria(query);
        V3JobQueryCriteriaEvaluator jobsPredicate = new V3JobQueryCriteriaEvaluator(criteria, titusRuntime);
        V3TaskQueryCriteriaEvaluator tasksPredicate = new V3TaskQueryCriteriaEvaluator(criteria, titusRuntime);

        Set<String> jobFields = newFieldsFilter(query.getJobFieldsList(), JOB_MINIMUM_FIELD_SET);
        Set<String> taskFields = newFieldsFilter(query.getTaskFieldsList(), TASK_MINIMUM_FIELD_SET);

        Flux<JobChangeNotification> eventStream = Flux.defer(() -> {
            AtomicBoolean first = new AtomicBoolean(true);
            return jobDataReplicator.events()
                    .subscribeOn(scheduler)
                    .publishOn(scheduler)
                    .flatMap(event -> {
                        JobManagerEvent<?> jobManagerEvent = event.getRight();

                        long now = titusRuntime.getClock().wallTime();
                        JobSnapshot snapshot = event.getLeft();
                        Optional<JobChangeNotification> grpcEvent = toObserveJobsEvent(snapshot, jobManagerEvent, now, jobsPredicate, tasksPredicate, jobFields, taskFields);

                        // On first event emit full snapshot first
                        if (first.getAndSet(false)) {
                            List<JobChangeNotification> snapshotEvents = buildSnapshot(snapshot, now, jobsPredicate, tasksPredicate, jobFields, taskFields);
                            grpcEvent.ifPresent(snapshotEvents::add);
                            return Flux.fromIterable(snapshotEvents);
                        }

                        // If we already emitted something we have to first disconnect this stream, and let the client
                        // subscribe again. Snapshot marker indicates that the underlying GRPC stream was disconnected.
                        if (jobManagerEvent == JobManagerEvent.snapshotMarker()) {
                            return Mono.error(new StatusRuntimeException(Status.ABORTED.augmentDescription(
                                    "Downstream event stream reconnected."
                            )));
                        }

                        // Job data replicator emits keep alive events periodically if there is nothing in the stream. We have
                        // to filter them out here.
                        if (jobManagerEvent instanceof JobKeepAliveEvent) {
                            // Check if staleness is not too high.
                            if (jobDataReplicator.getStalenessMs() > configuration.getObserveJobsStalenessDisconnectMs()) {
                                rejectedByStalenessTooHighMetric.increment();
                                return Mono.error(new StatusRuntimeException(Status.ABORTED.augmentDescription(
                                        "Data staleness in the event stream is too high. Most likely caused by connectivity issue to the downstream server."
                                )));
                            }
                            return Mono.empty();
                        }

                        return grpcEvent.map(Flux::just).orElseGet(Flux::empty);
                    });
        });
        return ReactorExt.toObservable(eventStream);
    }

    /**
     * Observable that completes when the job replicator cache checkpoint reaches the request timestamp.
     * It does not emit any value.
     */
    <T> Observable<T> syncCache(String endpoint, Class<T> type) {
        Flux<T> fluxSync = Flux.defer(() -> {
            long startTime = titusRuntime.getClock().wallTime();
            return jobDataReplicator.observeLastCheckpointTimestamp()
                    .subscribeOn(scheduler)
                    .publishOn(scheduler)
                    .skipUntil(timestamp -> timestamp >= startTime)
                    .take(1)
                    .flatMap(timestamp -> {
                        syncDelayMetric.withTags(endpoint).ifPresent(m -> {
                            long delayMs = titusRuntime.getClock().wallTime() - timestamp;
                            logger.debug("Cache sync delay: method={}, delaysMs={}", endpoint, delayMs);
                            m.recordLevel(delayMs);
                        });
                        return Mono.empty();
                    });
        });
        return ReactorExt.toObservable(fluxSync);
    }

    private Set<String> newFieldsFilter(List<String> fields, Set<String> minimumFieldSet) {
        return fields.isEmpty()
                ? Collections.emptySet()
                : CollectionsExt.merge(new HashSet<>(fields), minimumFieldSet);
    }

    private List<com.netflix.titus.api.jobmanager.model.job.Job> findMatchingJob(JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> queryCriteria) {
        JobSnapshot jobSnapshot = jobDataReplicator.getCurrent();
        Map<String, Job<?>> jobsById = jobSnapshot.getJobMap();

        V3JobQueryCriteriaEvaluator queryFilter = new V3JobQueryCriteriaEvaluator(queryCriteria, titusRuntime);

        List<com.netflix.titus.api.jobmanager.model.job.Job> matchingJobs = new ArrayList<>();
        jobsById.forEach((jobId, job) -> {
            List<com.netflix.titus.api.jobmanager.model.job.Task> tasks = new ArrayList<>(jobSnapshot.getTasks(jobId).values());
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
            Map<String, com.netflix.titus.api.jobmanager.model.job.Task> tasks = jobSnapshot.getTasks(jobId);
            if (!CollectionsExt.isNullOrEmpty(tasks)) {
                tasks.forEach((taskId, task) -> {
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
                                                      V3TaskQueryCriteriaEvaluator tasksPredicate,
                                                      Set<String> jobFields,
                                                      Set<String> taskFields) {
        List<JobChangeNotification> result = new ArrayList<>();
        Map<String, Job<?>> allJobsMap = snapshot.getJobMap();

        allJobsMap.forEach((jobId, job) -> {
            List<com.netflix.titus.api.jobmanager.model.job.Task> tasks = new ArrayList<>(snapshot.getTasks(jobId).values());
            if (jobsPredicate.test(Pair.of(job, tasks))) {
                result.add(toGrpcJobEvent(job, now, jobFields));
            }
        });

        snapshot.getTaskMap().forEach((taskId, task) -> {
            Job<?> job = allJobsMap.get(task.getJobId());
            if (job != null && tasksPredicate.test(Pair.of(job, task))) {
                result.add(toGrpcTaskEvent(task, false, now, taskFields));
            }
        });

        result.add(SNAPSHOT_END_MARKER);

        return result;
    }

    private Optional<JobChangeNotification> toObserveJobsEvent(JobSnapshot snapshot,
                                                               JobManagerEvent<?> event,
                                                               long now,
                                                               V3JobQueryCriteriaEvaluator jobsPredicate,
                                                               V3TaskQueryCriteriaEvaluator tasksPredicate,
                                                               Set<String> jobFields,
                                                               Set<String> taskFields) {
        if (event instanceof JobUpdateEvent) {
            JobUpdateEvent jobUpdateEvent = (JobUpdateEvent) event;
            Job<?> job = jobUpdateEvent.getCurrent();
            List<com.netflix.titus.api.jobmanager.model.job.Task> tasks = new ArrayList<>(snapshot.getTasks(job.getId()).values());
            return jobsPredicate.test(Pair.of(job, tasks)) ? Optional.of(toGrpcJobEvent(job, now, jobFields)) : Optional.empty();
        }

        if (event instanceof TaskUpdateEvent) {
            TaskUpdateEvent taskUpdateEvent = (TaskUpdateEvent) event;
            Job<?> job = taskUpdateEvent.getCurrentJob();
            com.netflix.titus.api.jobmanager.model.job.Task task = taskUpdateEvent.getCurrentTask();
            return tasksPredicate.test(Pair.of(job, task))
                    ? Optional.of(toGrpcTaskEvent(task, taskUpdateEvent.isMovedFromAnotherJob(), now, taskFields))
                    : Optional.empty();
        }
        return Optional.empty();
    }

    private JobChangeNotification toGrpcJobEvent(Job<?> job, long now, Set<String> jobFields) {
        com.netflix.titus.grpc.protogen.Job grpcJob = GrpcJobManagementModelConverters.toGrpcJob(job);
        if (!jobFields.isEmpty()) {
            grpcJob = ProtobufExt.copy(grpcJob, jobFields);
        }
        return JobChangeNotification.newBuilder()
                .setJobUpdate(JobChangeNotification.JobUpdate.newBuilder().setJob(grpcJob))
                .setTimestamp(now)
                .build();
    }

    private JobChangeNotification toGrpcTaskEvent(com.netflix.titus.api.jobmanager.model.job.Task task,
                                                  boolean movedFromAnotherJob,
                                                  long now,
                                                  Set<String> taskFields) {
        Task grpcTask = GrpcJobManagementModelConverters.toGrpcTask(task, logStorageInfo);
        if (!taskFields.isEmpty()) {
            grpcTask = ProtobufExt.copy(grpcTask, taskFields);
        }
        return JobChangeNotification.newBuilder()
                .setTaskUpdate(JobChangeNotification.TaskUpdate.newBuilder().setTask(grpcTask).setMovedFromAnotherJob(movedFromAnotherJob))
                .setTimestamp(now)
                .build();
    }
}
