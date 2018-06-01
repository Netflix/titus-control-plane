package com.netflix.titus.runtime.connector.jobmanager.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobStatus;
import com.netflix.titus.grpc.protogen.TaskStatus;
import com.netflix.titus.runtime.connector.jobmanager.JobCache;
import com.netflix.titus.runtime.connector.jobmanager.JobManagementClient;
import com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;

public class GrpcJobStreamCache implements JobStreamCache {

    private static final Logger logger = LoggerFactory.getLogger(GrpcJobStreamCache.class);

    private final JobManagementClient client;
    private final TitusRuntime titusRuntime;
    private final Scheduler scheduler;

    public GrpcJobStreamCache(JobManagementClient client, TitusRuntime titusRuntime, Scheduler scheduler) {
        this.client = client;
        this.titusRuntime = titusRuntime;
        this.scheduler = scheduler;
    }

    @Override
    public Observable<CacheEvent> connect() {
        return Observable.fromCallable(CacheUpdater::new)
                .flatMap(cacheUpdater -> client.observeJobs().flatMap(cacheUpdater::onEvent))
                .compose(ObservableExt.reemiter(
                        // If there are no events in the stream, we will periodically emit the last cache instance
                        // with the updated cache update timestamp, so it does not look stale.
                        cacheEvent -> new CacheEvent(cacheEvent.getCache(), titusRuntime.getClock().wallTime()),
                        LATENCY_REPORT_INTERVAL_MS, TimeUnit.MILLISECONDS,
                        scheduler
                ));
    }

    private class CacheUpdater {

        private List<JobChangeNotification> snapshotEvents = new ArrayList<>();
        private AtomicReference<JobCache> lastJobCacheRef = new AtomicReference<>();

        private Observable<CacheEvent> onEvent(JobChangeNotification event) {
            try {
                if (lastJobCacheRef.get() != null) {
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
                return Observable.error(e); // Return error to force the cache reconnect.
            }
            return Observable.empty();
        }

        private Observable<CacheEvent> buildInitialCache() {
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
                        tasksByJobId.computeIfAbsent(task.getJobId(), j -> new ArrayList<>()).add(V3GrpcModelConverters.toCoreTask(task));
                        break;
                }
            });

            // No longer needed
            snapshotEvents.clear();

            JobCache initialCache = new JobCache(jobsById, tasksByJobId);
            lastJobCacheRef.set(initialCache);

            logger.info("Job snapshot loaded: jobs={}, tasks={}", initialCache.getJobs().size(), initialCache.getTasks().size());

            return Observable.just(new CacheEvent(initialCache, titusRuntime.getClock().wallTime()));
        }

        private Observable<CacheEvent> processCacheUpdate(JobChangeNotification event) {
            JobCache lastCache = lastJobCacheRef.get();
            Optional<JobCache> newCache;
            switch (event.getNotificationCase()) {
                case JOBUPDATE:
                    Job job = V3GrpcModelConverters.toCoreJob(event.getJobUpdate().getJob());
                    newCache = lastCache.updateJob(job);
                    break;
                case TASKUPDATE:
                    Task task = V3GrpcModelConverters.toCoreTask(event.getTaskUpdate().getTask());
                    newCache = lastCache.updateTask(task);
                    break;
                default:
                    newCache = Optional.empty();
            }
            if (newCache.isPresent()) {
                lastJobCacheRef.set(newCache.get());
                return Observable.just(new CacheEvent(newCache.get(), titusRuntime.getClock().wallTime()));
            }
            return Observable.empty();
        }
    }
}
