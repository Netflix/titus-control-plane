package io.netflix.titus.testkit.client;

import java.util.HashMap;
import java.util.Map;

import com.netflix.titus.grpc.protogen.JobChangeNotification;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import io.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import io.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import io.netflix.titus.common.util.rx.ObservableExt;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
import rx.Observable;

public class V3ClientUtils {

    public static Observable<JobManagerEvent<?>> observeJobs(Observable<JobChangeNotification> grpcEvents) {
        return grpcEvents.filter(V3ClientUtils::isJobOrTaskUpdate)
                .compose(ObservableExt.mapWithState(new HashMap<>(), V3ClientUtils::toCoreEvent));
    }

    private static Pair<JobManagerEvent<?>, Map<String, Object>> toCoreEvent(JobChangeNotification event, Map<String, Object> state) {
        if (event.getNotificationCase() == JobChangeNotification.NotificationCase.JOBUPDATE) {
            Job<?> job = V3GrpcModelConverters.toCoreJob(event.getJobUpdate().getJob());

            Object previous = state.get(job.getId());
            state.put(job.getId(), job);

            if (previous == null) {
                return Pair.of(JobUpdateEvent.newJob(job), state);
            }
            return Pair.of(JobUpdateEvent.jobChange(job, (Job<?>) previous), state);
        }

        // Task update
        Task task = V3GrpcModelConverters.toCoreTask(event.getTaskUpdate().getTask());
        Job<?> job = (Job<?>) state.get(task.getJobId());

        Object previous = state.get(task.getId());
        state.put(task.getId(), task);

        if (previous == null) {
            return Pair.of(TaskUpdateEvent.newTask(job, task), state);
        }
        return Pair.of(TaskUpdateEvent.taskChange(job, task, (Task) previous), state);
    }

    private static boolean isJobOrTaskUpdate(JobChangeNotification event) {
        return event.getNotificationCase() == JobChangeNotification.NotificationCase.JOBUPDATE || event.getNotificationCase() == JobChangeNotification.NotificationCase.TASKUPDATE;
    }
}
