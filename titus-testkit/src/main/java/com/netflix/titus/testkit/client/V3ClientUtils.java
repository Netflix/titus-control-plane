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

package com.netflix.titus.testkit.client;

import java.util.HashMap;
import java.util.Map;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import com.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
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
        com.netflix.titus.grpc.protogen.Task grpcTask = event.getTaskUpdate().getTask();
        Job<?> job = (Job<?>) state.get(grpcTask.getJobId());

        Task task = V3GrpcModelConverters.toCoreTask(job, grpcTask);

        Object previous = state.get(task.getId());
        state.put(task.getId(), task);

        if (previous == null) {
            // TODO(fabio): handle isTaskMoved once added to the gRPC API
            return Pair.of(TaskUpdateEvent.newTask(job, task), state);
        }
        return Pair.of(TaskUpdateEvent.taskChange(job, task, (Task) previous), state);
    }

    private static boolean isJobOrTaskUpdate(JobChangeNotification event) {
        return event.getNotificationCase() == JobChangeNotification.NotificationCase.JOBUPDATE || event.getNotificationCase() == JobChangeNotification.NotificationCase.TASKUPDATE;
    }
}
