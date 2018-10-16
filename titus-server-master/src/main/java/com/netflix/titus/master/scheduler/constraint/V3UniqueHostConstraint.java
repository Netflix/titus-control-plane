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

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.titus.master.jobmanager.service.common.V3QueueableTask;

public class V3UniqueHostConstraint implements ConstraintEvaluator {

    public static final String NAME = "UniqueAgentConstraint";

    private static final Result VALID = new Result(true, null);
    private static final Result INVALID = new Result(false, "Task from the same job already running on the agent");

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        V3QueueableTask v3FenzoTask = (V3QueueableTask) taskRequest;
        String jobId = v3FenzoTask.getJob().getId();

        for (TaskRequest running : targetVM.getRunningTasks()) {
            if (check(jobId, running)) {
                return INVALID;
            }
        }

        for (TaskAssignmentResult assigned : targetVM.getTasksCurrentlyAssigned()) {
            if (check(jobId, assigned.getRequest())) {
                return INVALID;
            }
        }

        return VALID;
    }

    private boolean check(String jobId, TaskRequest running) {
        V3QueueableTask v3FenzoRunningTask = (V3QueueableTask) running;
        return v3FenzoRunningTask.getJob().getId().equals(jobId);
    }
}
