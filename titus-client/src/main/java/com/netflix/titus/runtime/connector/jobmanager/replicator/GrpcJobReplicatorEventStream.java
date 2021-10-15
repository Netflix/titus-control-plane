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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.Task;
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
import com.netflix.titus.runtime.connector.jobmanager.JobManagementClient;
import com.netflix.titus.runtime.connector.jobmanager.JobSnapshot;
import com.netflix.titus.runtime.connector.jobmanager.JobSnapshotFactory;
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
    private final JobDataReplicatorConfiguration configuration;

    private final ValueRangeCounter eventProcessingLatencies;
    private final AtomicInteger subscriptionCounter = new AtomicInteger();

    public GrpcJobReplicatorEventStream(JobManagementClient client,
                                        JobSnapshotFactory jobSnapshotFactory,
                                        JobDataReplicatorConfiguration configuration,
                                        DataReplicatorMetrics metrics,
                                        TitusRuntime titusRuntime,
                                        Scheduler scheduler) {
        this(client, Collections.emptyMap(), jobSnapshotFactory, configuration, metrics, titusRuntime, scheduler);
    }

    public GrpcJobReplicatorEventStream(JobManagementClient client,
                                        Map<String, String> filteringCriteria,
                                        JobSnapshotFactory jobSnapshotFactory,
                                        JobDataReplicatorConfiguration configuration,
                                        DataReplicatorMetrics metrics,
                                        TitusRuntime titusRuntime,
                                        Scheduler scheduler) {
        super(metrics, titusRuntime, scheduler);
        this.client = client;
        this.filteringCriteria = filteringCriteria;
        this.jobSnapshotFactory = jobSnapshotFactory;
        this.configuration = configuration;

        PolledMeter.using(titusRuntime.getRegistry()).withName(METRICS_ROOT + "activeSubscriptions").monitorValue(subscriptionCounter);
        this.eventProcessingLatencies = SpectatorExt.newValueRangeCounter(
                titusRuntime.getRegistry().createId(METRICS_ROOT + "processingLatency"),
                LEVELS,
                titusRuntime.getRegistry()
        );
    }

    @Override
    protected Flux<ReplicatorEvent<JobSnapshot, JobManagerEvent<?>>> newConnection() {
        return Flux
                .<ReplicatorEvent<JobSnapshot, JobManagerEvent<?>>>create(sink -> {
                    CacheUpdater cacheUpdater = new CacheUpdater(jobSnapshotFactory, titusRuntime);
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
        private final TitusRuntime titusRuntime;

        private final List<JobManagerEvent<?>> snapshotEvents = new ArrayList<>();
        private final AtomicReference<JobSnapshot> lastJobSnapshotRef = new AtomicReference<>();

        CacheUpdater(JobSnapshotFactory jobSnapshotFactory, TitusRuntime titusRuntime) {
            this.jobSnapshotFactory = jobSnapshotFactory;
            this.titusRuntime = titusRuntime;
        }

        Optional<ReplicatorEvent<JobSnapshot, JobManagerEvent<?>>> onEvent(JobManagerEvent<?> event) {
            if (logger.isDebugEnabled()) {
                if (event instanceof JobUpdateEvent) {
                    Job job = ((JobUpdateEvent) event).getCurrent();
                    logger.info("Received job update event: jobId={}, state={}, version={}", job.getId(), job.getStatus(), job.getVersion());
                } else if (event instanceof TaskUpdateEvent) {
                    Task task = ((TaskUpdateEvent) event).getCurrent();
                    logger.info("Received task update event: taskId={}, state={}, version={}", task.getId(), task.getStatus(), task.getVersion());
                } else if (event.equals(JobManagerEvent.snapshotMarker())) {
                    logger.info("Received snapshot marker");
                } else {
                    logger.info("Received unrecognized event type: {}", event);
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
            Map<String, Map<String, Task>> tasksByJobId = new HashMap<>();
            snapshotEvents.forEach(event -> {
                if (event instanceof JobUpdateEvent) {
                    Job<?> job = ((JobUpdateEvent) event).getCurrent();
                    jobs.put(job.getId(), job);
                } else if (event instanceof TaskUpdateEvent) {
                    TaskUpdateEvent taskUpdateEvent = (TaskUpdateEvent) event;
                    Task task = taskUpdateEvent.getCurrent();
                    Job<?> taskJob = jobs.get(task.getJobId());
                    if (taskJob != null) {
                        tasksByJobId.computeIfAbsent(task.getJobId(), t -> new HashMap<>()).put(task.getId(), task);
                    } else {
                        titusRuntime.getCodeInvariants().inconsistent("Job record not found: jobId=%s, taskId=%s", task.getJobId(), task.getId());
                    }
                }
            });

            // Filter out completed jobs. We could not do it sooner, as the job/task finished state could be proceeded by
            // earlier events so we have to collapse all of it.
            Iterator<Job<?>> jobIt = jobs.values().iterator();
            while (jobIt.hasNext()) {
                Job<?> job = jobIt.next();
                if (job.getStatus().getState() == JobState.Finished) {
                    jobIt.remove();
                    tasksByJobId.remove(job.getId());
                }
            }

            JobSnapshot initialSnapshot = jobSnapshotFactory.newSnapshot(jobs, tasksByJobId);

            // No longer needed
            snapshotEvents.clear();

            lastJobSnapshotRef.set(initialSnapshot);

            logger.info("Job snapshot loaded: {}", initialSnapshot.toSummaryString());

            return new ReplicatorEvent<>(initialSnapshot, JobManagerEvent.snapshotMarker(), titusRuntime.getClock().wallTime());
        }

        private Optional<ReplicatorEvent<JobSnapshot, JobManagerEvent<?>>> processCacheUpdate(JobManagerEvent<?> event) {
            JobSnapshot lastSnapshot = lastJobSnapshotRef.get();

            Optional<JobSnapshot> newSnapshot;
            JobManagerEvent<?> coreEvent = null;

            if (event instanceof JobUpdateEvent) {
                JobUpdateEvent jobUpdateEvent = (JobUpdateEvent) event;
                Job job = jobUpdateEvent.getCurrent();

                logger.debug("Processing job snapshot update event: updatedJobId={}", job.getId());

                newSnapshot = lastSnapshot.updateJob(job);
                coreEvent = toJobCoreEvent(job);
            } else if (event instanceof TaskUpdateEvent) {
                TaskUpdateEvent taskUpdateEvent = (TaskUpdateEvent) event;
                Task task = taskUpdateEvent.getCurrentTask();

                logger.debug("Processing job snapshot update event: updatedTaskId={}", task.getId());

                Optional<Job<?>> taskJobOpt = lastSnapshot.findJob(task.getJobId());
                if (taskJobOpt.isPresent()) {
                    Job<?> taskJob = taskJobOpt.get();
                    newSnapshot = lastSnapshot.updateTask(task, taskUpdateEvent.isMovedFromAnotherJob());
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
                return Optional.of(new ReplicatorEvent<>(newSnapshot.get(), coreEvent, titusRuntime.getClock().wallTime()));
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
