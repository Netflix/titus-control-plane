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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import com.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import com.netflix.titus.api.jobmanager.service.JobManagerConstants;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.runtime.connector.common.replicator.AbstractReplicatorEventStream;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicatorMetrics;
import com.netflix.titus.runtime.connector.common.replicator.ReplicatorEvent;
import com.netflix.titus.runtime.connector.jobmanager.JobManagementClient;
import com.netflix.titus.runtime.connector.jobmanager.JobSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

public class GrpcJobReplicatorEventStream extends AbstractReplicatorEventStream<JobSnapshot, JobManagerEvent<?>> {

    private static final Logger logger = LoggerFactory.getLogger(GrpcJobReplicatorEventStream.class);

    private final JobManagementClient client;

    public GrpcJobReplicatorEventStream(JobManagementClient client,
                                        DataReplicatorMetrics metrics,
                                        TitusRuntime titusRuntime,
                                        Scheduler scheduler) {
        super(metrics, titusRuntime, scheduler);
        this.client = client;
    }

    @Override
    protected Flux<ReplicatorEvent<JobSnapshot, JobManagerEvent<?>>> newConnection() {
        return Flux.defer(() -> {
            CacheUpdater cacheUpdater = new CacheUpdater();
            logger.info("Connecting to the job event stream...");
            return client.observeJobs(Collections.emptyMap()).flatMap(cacheUpdater::onEvent);
        });
    }

    private class CacheUpdater {

        private List<JobManagerEvent<?>> snapshotEvents = new ArrayList<>();
        private AtomicReference<JobSnapshot> lastJobSnapshotRef = new AtomicReference<>();

        private Flux<ReplicatorEvent<JobSnapshot, JobManagerEvent<?>>> onEvent(JobManagerEvent<?> event) {
            try {
                if (lastJobSnapshotRef.get() != null) {
                    return processCacheUpdate(event);
                }
                if (event.equals(JobManagerEvent.snapshotMarker())) {
                    return buildInitialCache();
                }

                if (event instanceof JobUpdateEvent) {
                    JobUpdateEvent jobUpdateEvent = (JobUpdateEvent) event;
                    Job job = jobUpdateEvent.getCurrent();
                    if (job.getStatus().getState() != JobState.Finished) {
                        snapshotEvents.add(event);
                    }
                } else if (event instanceof TaskUpdateEvent) {
                    TaskUpdateEvent taskUpdateEvent = (TaskUpdateEvent) event;
                    Task task = taskUpdateEvent.getCurrentTask();
                    if (task.getStatus().getState() != TaskState.Finished) {
                        snapshotEvents.add(event);
                    }
                }
            } catch (Exception e) {
                logger.warn("Unexpected error when handling the job change notification: {}", event, e);
                return Flux.error(e); // Return error to force the cache reconnect.
            }
            return Flux.empty();
        }

        private Flux<ReplicatorEvent<JobSnapshot, JobManagerEvent<?>>> buildInitialCache() {
            JobSnapshot.Builder builder = JobSnapshot.newBuilder(UUID.randomUUID().toString());

            snapshotEvents.forEach(event -> {
                if (event instanceof JobUpdateEvent) {
                    builder.addOrUpdateJob(((JobUpdateEvent) event).getCurrent());
                } else if (event instanceof TaskUpdateEvent) {
                    TaskUpdateEvent taskUpdateEvent = (TaskUpdateEvent) event;
                    Task task = taskUpdateEvent.getCurrent();
                    Job<?> taskJob = builder.getJob(task.getJobId());
                    if (taskJob != null) {
                        builder.addOrUpdateTask(task, taskUpdateEvent.isMovedFromAnotherJob());
                    } else {
                        titusRuntime.getCodeInvariants().inconsistent("Job record not found: jobId=%s, taskId=%s", task.getJobId(), task.getId());
                    }
                }
            });

            // No longer needed
            snapshotEvents.clear();

            JobSnapshot initialSnapshot = builder.build();
            lastJobSnapshotRef.set(initialSnapshot);

            logger.info("Job snapshot loaded: {}", initialSnapshot.toSummaryString());

            return Flux.just(new ReplicatorEvent<>(initialSnapshot, JobManagerEvent.snapshotMarker(), titusRuntime.getClock().wallTime()));
        }

        private Flux<ReplicatorEvent<JobSnapshot, JobManagerEvent<?>>> processCacheUpdate(JobManagerEvent<?> event) {
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
                return Flux.just(new ReplicatorEvent<>(newSnapshot.get(), coreEvent, titusRuntime.getClock().wallTime()));
            }
            return Flux.empty();
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
