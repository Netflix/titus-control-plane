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

package com.netflix.titus.runtime.connector.jobmanager.replicator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.event.JobKeepAliveEvent;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import com.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import com.netflix.titus.api.jobmanager.service.JobManagerConstants;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.common.util.spectator.SpectatorExt;
import com.netflix.titus.common.util.spectator.ValueRangeCounter;
import com.netflix.titus.runtime.connector.common.replicator.AbstractReplicatorEventStream;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicatorMetrics;
import com.netflix.titus.runtime.connector.common.replicator.ReplicatorEvent;
import com.netflix.titus.runtime.connector.jobmanager.JobConnectorConfiguration;
import com.netflix.titus.runtime.connector.jobmanager.JobManagementClient;
import com.netflix.titus.runtime.connector.jobmanager.RemoteJobManagementClientWithKeepAlive;
import com.netflix.titus.runtime.connector.jobmanager.snapshot.JobSnapshot;
import com.netflix.titus.runtime.connector.jobmanager.snapshot.JobSnapshotFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

public class GrpcJobReplicatorEventStream extends AbstractReplicatorEventStream<JobSnapshot, JobManagerEvent<?>> {

    private static final Logger logger = LoggerFactory.getLogger(GrpcJobReplicatorEventStream.class);

    private static final String METRICS_ROOT = "titus.grpcJobReplicatorEventStream.";

    private static final long[] LEVELS = new long[]{1, 10, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 60_000};

    private final JobManagementClient client;
    private final Map<String, String> filteringCriteria;
    private final JobSnapshotFactory jobSnapshotFactory;
    private final JobConnectorConfiguration configuration;

    // We read this property on startup as in some places it cannot be changed dynamically.
    private final boolean keepAliveEnabled;

    private final ValueRangeCounter eventProcessingLatencies;
    private final AtomicInteger subscriptionCounter = new AtomicInteger();

    public GrpcJobReplicatorEventStream(JobManagementClient client,
                                        JobSnapshotFactory jobSnapshotFactory,
                                        JobConnectorConfiguration configuration,
                                        DataReplicatorMetrics metrics,
                                        TitusRuntime titusRuntime,
                                        Scheduler scheduler) {
        this(client, Collections.emptyMap(), jobSnapshotFactory, configuration, metrics, titusRuntime, scheduler);
    }

    public GrpcJobReplicatorEventStream(JobManagementClient client,
                                        Map<String, String> filteringCriteria,
                                        JobSnapshotFactory jobSnapshotFactory,
                                        JobConnectorConfiguration configuration,
                                        DataReplicatorMetrics metrics,
                                        TitusRuntime titusRuntime,
                                        Scheduler scheduler) {
        super(client instanceof RemoteJobManagementClientWithKeepAlive, JobManagerEvent.keepAliveEvent(-1), metrics, titusRuntime, scheduler);
        this.client = client;
        this.filteringCriteria = filteringCriteria;
        this.jobSnapshotFactory = jobSnapshotFactory;
        this.configuration = configuration;
        this.keepAliveEnabled = configuration.isKeepAliveReplicatedStreamEnabled();

        PolledMeter.using(titusRuntime.getRegistry()).withName(METRICS_ROOT + "activeSubscriptions").monitorValue(subscriptionCounter);
        this.eventProcessingLatencies = SpectatorExt.newValueRangeCounterSortable(
                titusRuntime.getRegistry().createId(METRICS_ROOT + "processingLatency"),
                LEVELS,
                titusRuntime.getRegistry()
        );
    }

    @Override
    protected Flux<ReplicatorEvent<JobSnapshot, JobManagerEvent<?>>> newConnection() {
        return Flux
                .<ReplicatorEvent<JobSnapshot, JobManagerEvent<?>>>create(sink -> {
                    CacheUpdater cacheUpdater = new CacheUpdater(jobSnapshotFactory, keepAliveEnabled, titusRuntime);
                    logger.info("Connecting to the job event stream (filteringCriteria={})...", filteringCriteria);

                    ConnectableFlux<JobManagerEvent<?>> connectableStream = client.observeJobs(filteringCriteria).publish();
                    Flux<JobManagerEvent<?>> augmentedStream;
                    if (configuration.isConnectionTimeoutEnabled()) {
                        augmentedStream = Flux.merge(
                                connectableStream.take(1).timeout(Duration.ofMillis(configuration.getConnectionTimeoutMs())).ignoreElements()
                                        .onErrorMap(TimeoutException.class, error ->
                                                new TimeoutException(String.format("No event received from stream in %sms", configuration.getConnectionTimeoutMs()))
                                        ),
                                connectableStream
                        );
                    } else {
                        augmentedStream = connectableStream;
                    }
                    Disposable disposable = augmentedStream.subscribe(
                            jobEvent -> {
                                long started = titusRuntime.getClock().wallTime();
                                try {
                                    cacheUpdater.onEvent(jobEvent).ifPresent(sink::next);
                                    eventProcessingLatencies.recordLevel(titusRuntime.getClock().wallTime() - started);
                                } catch (Exception e) {
                                    // Throw error to force the cache reconnect.
                                    logger.warn("Unexpected error when handling the job change notification: {}", jobEvent, e);
                                    ExceptionExt.silent(() -> sink.error(e));
                                }
                            },
                            e -> ExceptionExt.silent(() -> sink.error(e)),
                            () -> ExceptionExt.silent(sink::complete)
                    );
                    sink.onDispose(disposable);
                    connectableStream.connect();
                })
                .doOnSubscribe(subscription -> subscriptionCounter.incrementAndGet())
                .doFinally(signal -> subscriptionCounter.decrementAndGet());
    }

    @VisibleForTesting
    static class CacheUpdater {

        private final JobSnapshotFactory jobSnapshotFactory;
        private final boolean archiveMode;
        private final TitusRuntime titusRuntime;

        private final long startTime;
        private final List<JobManagerEvent<?>> snapshotEvents = new ArrayList<>();
        private final AtomicReference<JobSnapshot> lastJobSnapshotRef = new AtomicReference<>();
        private final AtomicLong lastKeepAliveTimestamp;

        CacheUpdater(JobSnapshotFactory jobSnapshotFactory, boolean archiveMode, TitusRuntime titusRuntime) {
            this.jobSnapshotFactory = jobSnapshotFactory;
            this.archiveMode = archiveMode;
            this.titusRuntime = titusRuntime;
            this.startTime = titusRuntime.getClock().wallTime();
            this.lastKeepAliveTimestamp = new AtomicLong(startTime);
        }

        Optional<ReplicatorEvent<JobSnapshot, JobManagerEvent<?>>> onEvent(JobManagerEvent<?> event) {
            if (logger.isDebugEnabled()) {
                if (event instanceof JobUpdateEvent) {
                    Job job = ((JobUpdateEvent) event).getCurrent();
                    logger.debug("Received job update event: jobId={}, state={}, version={}", job.getId(), job.getStatus(), job.getVersion());
                } else if (event instanceof TaskUpdateEvent) {
                    Task task = ((TaskUpdateEvent) event).getCurrent();
                    logger.debug("Received task update event: taskId={}, state={}, version={}", task.getId(), task.getStatus(), task.getVersion());
                } else if (event.equals(JobManagerEvent.snapshotMarker())) {
                    logger.debug("Received snapshot marker");
                } else if (event instanceof JobKeepAliveEvent) {
                    logger.debug("Received job keep alive event: {}", event);
                } else {
                    logger.debug("Received unrecognized event type: {}", event);
                }
            }

            if (lastJobSnapshotRef.get() != null) {
                return processCacheUpdate(event);
            }
            if (event.equals(JobManagerEvent.snapshotMarker())) {
                return Optional.of(buildInitialCache());
            }

            // Snapshot event. Collect all of them before processing.
            if (event instanceof JobUpdateEvent || event instanceof TaskUpdateEvent) {
                snapshotEvents.add(event);
            }

            return Optional.empty();
        }

        private ReplicatorEvent<JobSnapshot, JobManagerEvent<?>> buildInitialCache() {
            Map<String, Job<?>> jobs = new HashMap<>();
            Set<String> jobsToRemove = new HashSet<>();

            snapshotEvents.forEach(event -> {
                if (event instanceof JobUpdateEvent) {
                    Job<?> job = ((JobUpdateEvent) event).getCurrent();
                    jobs.put(job.getId(), job);
                    if (job.getStatus().getState() == JobState.Finished) {
                        // If archive mode is disabled, we always remove finished jobs immediately.
                        if (!archiveMode || event.isArchived()) {
                            jobsToRemove.add(job.getId());
                        }
                    }
                }
            });

            // Filter out completed jobs. We could not do it sooner, as the job/task finished state could be proceeded by
            // earlier events so we have to collapse all of it.
            jobs.keySet().removeAll(jobsToRemove);

            Map<String, Task> tasks = new HashMap<>();
            Set<String> tasksToRemove = new HashSet<>();
            snapshotEvents.forEach(event -> {
                if (event instanceof TaskUpdateEvent) {
                    TaskUpdateEvent taskUpdateEvent = (TaskUpdateEvent) event;
                    Task task = taskUpdateEvent.getCurrent();
                    Job<?> taskJob = jobs.get(task.getJobId());

                    if (taskJob != null) {
                        tasks.put(task.getId(), task);
                        if (task.getStatus().getState() == TaskState.Finished) {
                            // If archive mode is disabled, we cannot make a good decision about a finished task so we have to keep it.
                            if (archiveMode) {
                                tasksToRemove.add(task.getId());
                            }
                        }
                    } else {
                        if (!jobsToRemove.contains(task.getJobId())) {
                            titusRuntime.getCodeInvariants().inconsistent("Job record not found: jobId=%s, taskId=%s", task.getJobId(), task.getId());
                        }
                    }
                }
            });
            tasks.keySet().removeAll(tasksToRemove);

            Map<String, Map<String, Task>> tasksByJobId = new HashMap<>();
            tasks.forEach((taskId, task) -> tasksByJobId.computeIfAbsent(task.getJobId(), t -> new HashMap<>()).put(task.getId(), task));

            JobSnapshot initialSnapshot = jobSnapshotFactory.newSnapshot(jobs, tasksByJobId);

            // No longer needed
            snapshotEvents.clear();

            lastJobSnapshotRef.set(initialSnapshot);

            logger.info("Job snapshot loaded: {}", initialSnapshot.toSummaryString());

            // Checkpoint timestamp for the cache event, is the time just before establishing the connection.
            return new ReplicatorEvent<>(initialSnapshot, JobManagerEvent.snapshotMarker(), titusRuntime.getClock().wallTime(), startTime);
        }

        private Optional<ReplicatorEvent<JobSnapshot, JobManagerEvent<?>>> processCacheUpdate(JobManagerEvent<?> event) {
            JobSnapshot lastSnapshot = lastJobSnapshotRef.get();

            if (event instanceof JobKeepAliveEvent) {
                JobKeepAliveEvent keepAliveEvent = (JobKeepAliveEvent) event;
                lastKeepAliveTimestamp.set(keepAliveEvent.getTimestamp());
                return Optional.of(new ReplicatorEvent<>(lastSnapshot, event, titusRuntime.getClock().wallTime(), keepAliveEvent.getTimestamp()));
            }

            Optional<JobSnapshot> newSnapshot;
            JobManagerEvent<?> coreEvent = null;

            if (event instanceof JobUpdateEvent) {
                JobUpdateEvent jobUpdateEvent = (JobUpdateEvent) event;
                Job job = jobUpdateEvent.getCurrent();

                logger.debug("Processing job snapshot update event: updatedJobId={}", job.getId());

                if (archiveMode && jobUpdateEvent.isArchived()) {
                    newSnapshot = lastSnapshot.removeArchivedJob(job);
                } else {
                    newSnapshot = lastSnapshot.updateJob(job);
                }
                coreEvent = toJobCoreEvent(job);
            } else if (event instanceof TaskUpdateEvent) {
                TaskUpdateEvent taskUpdateEvent = (TaskUpdateEvent) event;
                Task task = taskUpdateEvent.getCurrentTask();

                logger.debug("Processing job snapshot update event: updatedTaskId={}", task.getId());

                Optional<Job<?>> taskJobOpt = lastSnapshot.findJob(task.getJobId());
                if (taskJobOpt.isPresent()) {
                    Job<?> taskJob = taskJobOpt.get();
                    if (archiveMode && taskUpdateEvent.isArchived()) {
                        newSnapshot = lastSnapshot.removeArchivedTask(task);
                    } else {
                        newSnapshot = lastSnapshot.updateTask(task, taskUpdateEvent.isMovedFromAnotherJob());
                    }
                    coreEvent = toTaskCoreEvent(taskJob, task, taskUpdateEvent.isMovedFromAnotherJob());
                } else {
                    titusRuntime.getCodeInvariants().inconsistent("Job record not found: jobId=%s, taskId=%s", task.getJobId(), task.getId());
                    newSnapshot = Optional.empty();
                }
            } else {
                newSnapshot = Optional.empty();
            }
            if (newSnapshot.isPresent()) {
                lastJobSnapshotRef.set(newSnapshot.get());
                return Optional.of(new ReplicatorEvent<>(newSnapshot.get(), coreEvent, titusRuntime.getClock().wallTime(), lastKeepAliveTimestamp.get()));
            }
            return Optional.empty();
        }

        private JobManagerEvent<?> toJobCoreEvent(Job newJob) {
            return lastJobSnapshotRef.get().findJob(newJob.getId())
                    .map(previousJob -> JobUpdateEvent.jobChange(newJob, previousJob, JobManagerConstants.GRPC_REPLICATOR_CALL_METADATA))
                    .orElseGet(() -> JobUpdateEvent.newJob(newJob, JobManagerConstants.GRPC_REPLICATOR_CALL_METADATA));
        }

        private JobManagerEvent<?> toTaskCoreEvent(Job<?> job, Task newTask, boolean moved) {
            if (moved) {
                return TaskUpdateEvent.newTaskFromAnotherJob(job, newTask, JobManagerConstants.GRPC_REPLICATOR_CALL_METADATA);
            }
            return lastJobSnapshotRef.get().findTaskById(newTask.getId())
                    .map(jobTaskPair -> TaskUpdateEvent.taskChange(job, newTask, jobTaskPair.getRight(), JobManagerConstants.GRPC_REPLICATOR_CALL_METADATA))
                    .orElseGet(() -> TaskUpdateEvent.newTask(job, newTask, JobManagerConstants.GRPC_REPLICATOR_CALL_METADATA));
        }
    }
}
