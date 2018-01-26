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

package io.netflix.titus.master.scheduler.fitness;

import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.VirtualMachineCurrentState;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.model.v2.V2JobState;
import io.netflix.titus.api.store.v2.V2WorkerMetadata;
import io.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
import io.netflix.titus.master.scheduler.ScheduledRequest;

/**
 * A fitness calculator that will prefer placing tasks on nodes that have the least amount of tasks
 * launching in order to reduce concurrent task launches. Nodes without any tasks will return a low score
 * as we only want to use empty nodes if there are no other nodes available.
 */
public class TaskLaunchingFitnessCalculator implements VMTaskFitnessCalculator {

    private static final double EMPTY_HOST_SCORE = 0.01;
    private static final double LAUNCHING_TASKS_SCORE = 0.5;
    private static final double NOT_LAUNCHING_TASKS_SCORE = 1.0;

    @Override
    public String getName() {
        return "Task Launching Fitness Calculator";
    }

    @Override
    public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        int totalTasks = targetVM.getRunningTasks().size();
        int totalLaunchingTasks = (int) targetVM.getRunningTasks().stream().filter(this::isTaskLaunching).count();

        int totalAssignedTasks = targetVM.getTasksCurrentlyAssigned().size();
        totalTasks += totalAssignedTasks;
        totalLaunchingTasks += totalAssignedTasks;

        if (totalTasks == 0) {
            return EMPTY_HOST_SCORE;
        } else if (totalLaunchingTasks == 0) {
            return NOT_LAUNCHING_TASKS_SCORE;
        }

        double launchingTasksRatio = 1.0 / (double) totalLaunchingTasks;
        return launchingTasksRatio * LAUNCHING_TASKS_SCORE;
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
