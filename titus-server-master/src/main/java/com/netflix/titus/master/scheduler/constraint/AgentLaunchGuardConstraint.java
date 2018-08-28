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

package com.netflix.titus.master.scheduler.constraint;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;

/**
 * A system constraint that prevents launching a task on an agent that already has a task launching.
 */
@Singleton
public class AgentLaunchGuardConstraint implements SystemConstraint {

    public static final String NAME = "AgentLaunchGuardConstraint";

    private static final Result VALID = new Result(true, null);
    private static final Result INVALID = new Result(false, "The agent has a task already launching");

    private final SchedulerConfiguration schedulerConfiguration;
    private final V3JobOperations v3JobOperations;

    private final AtomicReference<Map<String, Task>> taskIdMapRef = new AtomicReference<>(Collections.emptyMap());

    @Inject
    public AgentLaunchGuardConstraint(SchedulerConfiguration schedulerConfiguration,
                                      V3JobOperations v3JobOperations) {
        this.schedulerConfiguration = schedulerConfiguration;
        this.v3JobOperations = v3JobOperations;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void prepare() {
        Map<String, Task> taskIdMap = new HashMap<>();
        for (Task task : v3JobOperations.getTasks()) {
            taskIdMap.put(task.getId(), task);
        }
        taskIdMapRef.set(taskIdMap);
    }

    @Override
    public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        if (!schedulerConfiguration.isGlobalTaskLaunchingConstraintEvaluatorEnabled()) {
            return VALID;
        }

        if (!targetVM.getTasksCurrentlyAssigned().isEmpty()) {
            return INVALID;
        }

        return hasLaunchingTask(targetVM) ? INVALID : VALID;
    }

    public static boolean isAgentLaunchGuardConstraintReason(String reason) {
        return reason != null && INVALID.getFailureReason().contains(reason);
    }

    private boolean hasLaunchingTask(VirtualMachineCurrentState targetVM) {
        for (TaskRequest running : targetVM.getRunningTasks()) {
            if (isTaskLaunching(running)) {
                return true;
            }
        }
        return false;
    }

    private boolean isTaskLaunching(TaskRequest request) {
        Task current = taskIdMapRef.get().get(request.getId());
        if (current == null) {
            return false;
        }
        TaskState state = current.getStatus().getState();
        return state == TaskState.Accepted || state == TaskState.Launched || state == TaskState.StartInitiated;
    }
}
