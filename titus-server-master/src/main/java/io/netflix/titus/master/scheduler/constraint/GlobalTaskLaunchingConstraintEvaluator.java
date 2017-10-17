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

package io.netflix.titus.master.scheduler.constraint;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.model.v2.V2JobState;
import io.netflix.titus.api.store.v2.V2WorkerMetadata;
import io.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
import io.netflix.titus.master.scheduler.ScheduledRequest;
import io.netflix.titus.master.scheduler.SchedulerConfiguration;

/**
 * A global constraint evaluator that prevents launching a task on a node that already has a task launching.
 */
@Singleton
public class GlobalTaskLaunchingConstraintEvaluator implements GlobalConstraintEvaluator {

    private final SchedulerConfiguration schedulerConfiguration;

    @Inject
    public GlobalTaskLaunchingConstraintEvaluator(SchedulerConfiguration schedulerConfiguration) {
        this.schedulerConfiguration = schedulerConfiguration;
    }

    @Override
    public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        if (schedulerConfiguration.isGlobalTaskLaunchingConstraintEvaluatorEnabled()) {
            int totalLaunchingTasks = (int) targetVM.getRunningTasks().stream().filter(this::isTaskLaunching).count();
            int totalAssignedTasks = targetVM.getTasksCurrentlyAssigned().size();
            totalLaunchingTasks += totalAssignedTasks;

            if (totalLaunchingTasks > 0) {
                return new Result(false, targetVM.getHostname() + " has a task already launching");
            }
        }
        return new Result(true, "");
    }

    private boolean isTaskLaunching(TaskRequest request) {
        if (request instanceof ScheduledRequest) {
            V2WorkerMetadata task = ((ScheduledRequest) request).getTask();
            V2JobState state = task.getState();
            return state == V2JobState.Accepted || state == V2JobState.Launched || state == V2JobState.StartInitiated;
        } else if (request instanceof V3QueueableTask) {
            Task task = ((V3QueueableTask) request).getTask();
            TaskState state = task.getStatus().getState();
            return state == TaskState.Accepted || state == TaskState.Launched || state == TaskState.StartInitiated;
        }
        return false;
    }
}
