package com.netflix.titus.runtime.connector.jobmanager.replicator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobStatus;
import com.netflix.titus.grpc.protogen.TaskStatus;
import com.netflix.titus.runtime.connector.common.replicator.AbstractReplicatorEventStream;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicatorMetrics;
import com.netflix.titus.runtime.connector.jobmanager.JobManagementClient;
import com.netflix.titus.runtime.connector.jobmanager.JobSnapshot;
import com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

public class GrpcJobReplicatorEventStream extends AbstractReplicatorEventStream<JobSnapshot> {

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
    protected Flux<ReplicatorEvent<JobSnapshot>> newConnection() {
        return Flux.defer(() -> {
            CacheUpdater cacheUpdater = new CacheUpdater();
            logger.info("Connecting to the job event stream...");
            return ReactorExt.toFlux(client.observeJobs()).flatMap(cacheUpdater::onEvent);
        });
    }

    private class CacheUpdater {

        private List<JobChangeNotification> snapshotEvents = new ArrayList<>();
        private AtomicReference<JobSnapshot> lastJobSnapshotRef = new AtomicReference<>();

        private Flux<ReplicatorEvent<JobSnapshot>> onEvent(JobChangeNotification event) {
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

        private Flux<ReplicatorEvent<JobSnapshot>> buildInitialCache() {
            Map<String, Job<?>> jobsById = new HashMap<>();
            Map<String, List<Task>> tasksByJobId = new HashMap<>();

            snapshotEvents.forEach(event -> {
                switch (event.getNotificationCase()) {
                    case JOBUPDATE:
                        com.netflix.titus.grpc.protogen.Job job = event.getJobUpdate().getJob();
                        jobsById.put(job.getId(), V3GrpcModelConverters.toCoreJob(job));
                        break;
                    case TASKUPDATE:
                        com.netflix.titus.grpc.protogen.Task task = event.getTaskUpdate().getTask();
                        Job<?> taskJob = jobsById.get(task.getJobId());
                        if (taskJob != null) {
                            tasksByJobId.computeIfAbsent(task.getJobId(), j -> new ArrayList<>()).add(V3GrpcModelConverters.toCoreTask(taskJob, task));
                        } else {
                            titusRuntime.getCodeInvariants().inconsistent("Job record not found: jobId=%s, taskId=%s", task.getJobId(), task.getId());
                        }
                        break;
                }
            });

            // No longer needed
            snapshotEvents.clear();

            JobSnapshot initialSnapshot = new JobSnapshot(jobsById, tasksByJobId);
            lastJobSnapshotRef.set(initialSnapshot);

            logger.info("Job snapshot loaded: jobs={}, tasks={}", initialSnapshot.getJobs().size(), initialSnapshot.getTasks().size());

            return Flux.just(new ReplicatorEvent<>(initialSnapshot, titusRuntime.getClock().wallTime()));
        }

        private Flux<ReplicatorEvent<JobSnapshot>> processCacheUpdate(JobChangeNotification event) {
            JobSnapshot lastSnapshot = lastJobSnapshotRef.get();
            Optional<JobSnapshot> newSnapshot;
            switch (event.getNotificationCase()) {
                case JOBUPDATE:
                    Job job = V3GrpcModelConverters.toCoreJob(event.getJobUpdate().getJob());
                    newSnapshot = lastSnapshot.updateJob(job);
                    break;
                case TASKUPDATE:
                    com.netflix.titus.grpc.protogen.Task task = event.getTaskUpdate().getTask();
                    Job<?> taskJob = lastSnapshot.getJobs().stream().filter(j -> j.getId().equals(task.getJobId())).findFirst().orElse(null);
                    if (taskJob != null) {
                        newSnapshot = lastSnapshot.updateTask(V3GrpcModelConverters.toCoreTask(taskJob, task));
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
                return Flux.just(new ReplicatorEvent<>(newSnapshot.get(), titusRuntime.getClock().wallTime()));
            }
            return Flux.empty();
        }
    }
}
