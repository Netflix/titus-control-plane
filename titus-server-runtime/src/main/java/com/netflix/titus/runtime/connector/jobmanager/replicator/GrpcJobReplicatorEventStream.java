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
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import com.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobStatus;
import com.netflix.titus.grpc.protogen.ObserveJobsQuery;
import com.netflix.titus.grpc.protogen.TaskStatus;
import com.netflix.titus.runtime.connector.common.replicator.AbstractReplicatorEventStream;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicatorMetrics;
import com.netflix.titus.runtime.connector.common.replicator.ReplicatorEvent;
import com.netflix.titus.runtime.connector.jobmanager.JobManagementClient;
import com.netflix.titus.runtime.connector.jobmanager.JobSnapshot;
import com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
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
            return ReactorExt.toFlux(client.observeJobs(ObserveJobsQuery.getDefaultInstance()))
                    .flatMap(cacheUpdater::onEvent);
        });
    }

    private class CacheUpdater {

        private List<JobChangeNotification> snapshotEvents = new ArrayList<>();
        private AtomicReference<JobSnapshot> lastJobSnapshotRef = new AtomicReference<>();

        private Flux<ReplicatorEvent<JobSnapshot, JobManagerEvent<?>>> onEvent(JobChangeNotification event) {
            try {
                if (lastJobSnapshotRef.get() != null) {
                    return processCacheUpdate(event);
                }
                if (event.getNotificationCase() == JobChangeNotification.NotificationCase.SNAPSHOTEND) {
                    return buildInitialCache();
                }

                switch (event.getNotificationCase()) {
                    case JOBUPDATE:
                        if (event.getJobUpdate().getJob().getStatus().getState() != JobStatus.JobState.Finished) {
                            snapshotEvents.add(event);
                        }
                        break;
                    case TASKUPDATE:
                        if (event.getTaskUpdate().getTask().getStatus().getState() != TaskStatus.TaskState.Finished) {
                            snapshotEvents.add(event);
                        }
                        break;
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
                switch (event.getNotificationCase()) {
                    case JOBUPDATE:
                        com.netflix.titus.grpc.protogen.Job job = event.getJobUpdate().getJob();
                        builder.addOrUpdateJob(V3GrpcModelConverters.toCoreJob(job));
                        break;
                    case TASKUPDATE:
                        com.netflix.titus.grpc.protogen.Task task = event.getTaskUpdate().getTask();
                        Job<?> taskJob = builder.getJob(task.getJobId());
                        if (taskJob != null) {
                            builder.addOrUpdateTask(V3GrpcModelConverters.toCoreTask(taskJob, task), event.getTaskUpdate().getMovedFromAnotherJob());
                        } else {
                            titusRuntime.getCodeInvariants().inconsistent("Job record not found: jobId=%s, taskId=%s", task.getJobId(), task.getId());
                        }
                        break;
                }
            });

            // No longer needed
            snapshotEvents.clear();

            JobSnapshot initialSnapshot = builder.build();
            lastJobSnapshotRef.set(initialSnapshot);

            logger.info("Job snapshot loaded: jobs={}, tasks={}", initialSnapshot.getJobs().size(), initialSnapshot.getTasks().size());

            return Flux.just(new ReplicatorEvent<>(initialSnapshot, JobManagerEvent.snapshotMarker(), titusRuntime.getClock().wallTime()));
        }

        private Flux<ReplicatorEvent<JobSnapshot, JobManagerEvent<?>>> processCacheUpdate(JobChangeNotification event) {
            JobSnapshot lastSnapshot = lastJobSnapshotRef.get();

            Optional<JobSnapshot> newSnapshot;
            JobManagerEvent<?> coreEvent = null;

            switch (event.getNotificationCase()) {
                case JOBUPDATE:
                    Job job = V3GrpcModelConverters.toCoreJob(event.getJobUpdate().getJob());
                    newSnapshot = lastSnapshot.updateJob(job);
                    coreEvent = toJobCoreEvent(job);
                    break;
                case TASKUPDATE:
                    com.netflix.titus.grpc.protogen.Task task = event.getTaskUpdate().getTask();
                    Optional<Job<?>> taskJobOpt = lastSnapshot.findJob(task.getJobId());
                    if (taskJobOpt.isPresent()) {
                        Job<?> taskJob = taskJobOpt.get();
                        Task coreTask = V3GrpcModelConverters.toCoreTask(taskJob, task);
                        newSnapshot = lastSnapshot.updateTask(coreTask, event.getTaskUpdate().getMovedFromAnotherJob());
                        coreEvent = toTaskCoreEvent(taskJob, coreTask, event.getTaskUpdate().getMovedFromAnotherJob());
                    } else {
                        titusRuntime.getCodeInvariants().inconsistent("Job record not found: jobId=%s, taskId=%s", task.getJobId(), task.getId());
                        newSnapshot = Optional.empty();
                    }
                    break;
                default:
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
                    .map(previousJob -> JobUpdateEvent.jobChange(newJob, previousJob))
                    .orElseGet(() -> JobUpdateEvent.newJob(newJob));
        }

        private JobManagerEvent<?> toTaskCoreEvent(Job<?> job, Task newTask, boolean moved) {
            if (moved) {
                return TaskUpdateEvent.newTaskFromAnotherJob(job, newTask);
            }
            return lastJobSnapshotRef.get().findTaskById(newTask.getId())
                    .map(jobTaskPair -> TaskUpdateEvent.taskChange(job, newTask, jobTaskPair.getRight()))
                    .orElseGet(() -> TaskUpdateEvent.newTask(job, newTask));
        }
    }
}
