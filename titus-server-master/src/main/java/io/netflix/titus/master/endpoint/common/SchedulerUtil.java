/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.endpoint.common;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.fenzo.queues.QueuableTask;
import com.netflix.fenzo.queues.TaskQueue;
import io.netflix.titus.master.scheduler.SchedulingService;
import rx.Observable;
import rx.schedulers.Schedulers;

public class SchedulerUtil {

    public static Map<TaskQueue.TaskState, Collection<QueuableTask>> blockAndGetTasksFromQueue(
            SchedulingService schedulingService
    ) {
        return Observable.create((Observable.OnSubscribe<Map<TaskQueue.TaskState, Collection<QueuableTask>>>) s ->
                schedulingService.registerTaskQListAction(s::onNext))
                .observeOn(Schedulers.computation()) // separate out processing of the results to different worker thread
                .take(1)
                .timeout(30, TimeUnit.SECONDS)
                .onErrorReturn(throwable -> null) // timeout will produce onError()
                .toBlocking()
                .first();
    }

    public static List<TaskAssignmentResult> blockAndGetTaskAssignmentFailures(
            SchedulingService schedulingService,
            String taskId
    ) {
        return Observable.create((Observable.OnSubscribe<List<TaskAssignmentResult>>) s ->
                schedulingService.registerTaskFailuresAction(taskId, s::onNext))
                .observeOn(Schedulers.computation()) // separate out processing of the results to different worker thread
                .take(1)
                .timeout(30, TimeUnit.SECONDS)
                .onErrorReturn(t ->
                        t instanceof IllegalStateException ? Collections.singletonList(null) : null) // timeout will produce onError()
                .toBlocking()
                .first();
    }
}
