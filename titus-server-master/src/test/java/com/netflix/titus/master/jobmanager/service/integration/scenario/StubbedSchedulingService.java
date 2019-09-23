/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.master.jobmanager.service.integration.scenario;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.base.Preconditions;
import com.netflix.fenzo.TaskRequest;
import com.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
import com.netflix.titus.master.scheduler.SchedulingResultEvent;
import com.netflix.titus.master.scheduler.SchedulingService;
import com.netflix.titus.master.scheduler.TaskPlacementFailure;
import rx.Observable;

class StubbedSchedulingService implements SchedulingService<TaskRequest> {

    private final Map<String, V3QueueableTask> queuableTasks = new HashMap<>();

    public HashMap<String, V3QueueableTask> getQueuableTasks() {
        return new HashMap<>(queuableTasks);
    }

    @Override
    public void addTask(V3QueueableTask queuableTask) {
        queuableTasks.put(queuableTask.getId(), queuableTask);
    }

    @Override
    public void removeTask(String taskId) {
        Preconditions.checkArgument(queuableTasks.containsKey(taskId));
        queuableTasks.remove(taskId);
    }

    @Override
    public void addRunningTask(V3QueueableTask task) {
        queuableTasks.put(task.getId(), task);
    }

    @Override
    public Optional<SchedulingResultEvent> findLastSchedulingResult(String taskId) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Observable<SchedulingResultEvent> observeSchedulingResults(String taskId) {
        return Observable.error(new UnsupportedOperationException("not implemented"));
    }

    @Override
    public Map<TaskPlacementFailure.FailureKind, Map<TaskRequest, List<TaskPlacementFailure>>> getLastTaskPlacementFailures() {
        return Collections.emptyMap();
    }
}
