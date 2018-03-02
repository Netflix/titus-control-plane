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

package io.netflix.titus.master.scheduler;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.fenzo.TaskScheduler;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.queues.QAttributes;
import com.netflix.fenzo.queues.QueuableTask;
import com.netflix.fenzo.queues.TaskQueue;
import io.netflix.titus.api.model.v2.JobConstraints;
import io.netflix.titus.master.scheduler.constraint.ConstraintEvaluatorTransformer;
import io.netflix.titus.master.scheduler.constraint.SystemHardConstraint;
import io.netflix.titus.master.scheduler.constraint.SystemSoftConstraint;
import rx.functions.Action1;

/**
 */
public interface SchedulingService {

    String COMPONENT = "scheduler";

    /**
     * FIXME Starting the scheduler explicitly is a workaround needed because of the circular dependencies between components.
     */
    void startScheduling();

    TaskScheduler getTaskScheduler();

    List<VirtualMachineCurrentState> getVmCurrentStates();

    Action1<QueuableTask> getTaskQueueAction();

    void removeTask(String taskid, QAttributes qAttributes, String hostname);

    void initRunningTask(QueuableTask task, String hostname);

    SystemSoftConstraint getSystemSoftConstraint();

    SystemHardConstraint getSystemHardConstraint();

    ConstraintEvaluatorTransformer<JobConstraints> getV2ConstraintEvaluatorTransformer();

    /**
     * Register an action to receive list of tasks in Fenzo. The action is called when results from the latest
     * scheduling iteration are available. Although this call does not slow down any ongoing scheduling iteration,
     * the launch of the next scheuling iteration is delayed until this method returns. Therefore, this call must return
     * quickly. For example, callers may wish to asynchronously process the results made available to them via the
     * supplied action.
     *
     * @param action The action to call when results are available.
     * @throws IllegalStateException If there are too many concurrent requests.
     */
    void registerTaskQListAction(
            com.netflix.fenzo.functions.Action1<Map<TaskQueue.TaskState, Collection<QueuableTask>>> action
    ) throws IllegalStateException;

    /**
     * Register an action to receive list of task assignment failures for a task from the latest scheduling iteration.
     * Although this call does not slow down any ongoing scheduling iteration,
     * the launch of the next scheuling iteration is delayed until this method returns. Therefore, this call must return
     * quickly. For example, callers may wish to asynchronously process the results made available to them via the
     * supplied action.
     *
     * @param taskId The task Id for which assignment failures are needed.
     * @param action The action to call when results are available with list of assignment failures for the task, or with
     *               <code>null</code> if the task isn't pending in the queue for resource assignment.
     * @throws IllegalStateException If there are too many concurrent requests.
     */
    void registerTaskFailuresAction(
            String taskId, com.netflix.fenzo.functions.Action1<List<TaskAssignmentResult>> action
    ) throws IllegalStateException;
}
