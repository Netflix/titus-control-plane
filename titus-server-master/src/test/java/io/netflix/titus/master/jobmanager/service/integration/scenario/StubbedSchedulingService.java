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

package io.netflix.titus.master.jobmanager.service.integration.scenario;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.fenzo.TaskScheduler;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.queues.QAttributes;
import com.netflix.fenzo.queues.QueuableTask;
import com.netflix.fenzo.queues.TaskQueue;
import io.netflix.titus.master.scheduler.V2ConstraintEvaluatorTransformer;
import io.netflix.titus.master.scheduler.SchedulingService;
import io.netflix.titus.master.scheduler.constraint.GlobalConstraintEvaluator;
import rx.functions.Action1;

class StubbedSchedulingService implements SchedulingService {

    private final Map<String, QueuableTask> queuableTasks = new HashMap<>();

    @Override
    public TaskScheduler getTaskScheduler() {
        return null;
    }

    @Override
    public List<VirtualMachineCurrentState> getVmCurrentStates() {
        throw new IllegalStateException("not implemented");
    }

    public void addQueuableTask(QueuableTask queuableTask) {
        queuableTasks.put(queuableTask.getId(), queuableTask);
    }

    public HashMap<String, QueuableTask> getQueuableTasks() {
        return new HashMap<>(queuableTasks);
    }

    @Override
    public Action1<QueuableTask> getTaskQueueAction() {
        return this::addQueuableTask;
    }

    @Override
    public void removeTask(String taskid, QAttributes qAttributes, String hostname) {
        Preconditions.checkArgument(queuableTasks.containsKey(taskid));
        queuableTasks.remove(taskid);
    }

    @Override
    public void initRunningTask(QueuableTask task, String hostname) {
        throw new IllegalStateException("not implemented yet");
    }

    @Override
    public GlobalConstraintEvaluator getGlobalConstraints() {
        return null;
    }

    @Override
    public V2ConstraintEvaluatorTransformer getV2ConstraintEvaluatorTransformer() {
        return null;
    }

    @Override
    public void registerTaskQListAction(com.netflix.fenzo.functions.Action1<Map<TaskQueue.TaskState, Collection<QueuableTask>>> action) throws IllegalStateException {
    }

    @Override
    public void registerTaskFailuresAction(String taskId, com.netflix.fenzo.functions.Action1<List<TaskAssignmentResult>> action) throws IllegalStateException {
    }
}
