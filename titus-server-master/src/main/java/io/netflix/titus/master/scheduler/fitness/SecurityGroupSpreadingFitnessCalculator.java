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

package io.netflix.titus.master.scheduler.fitness;

import java.util.List;

import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.VirtualMachineCurrentState;

import static io.netflix.titus.master.scheduler.fitness.FitnessCalculatorFunctions.countMatchingTasks;
import static io.netflix.titus.master.scheduler.fitness.FitnessCalculatorFunctions.getAllTasksOnAgent;

/**
 * A fitness calculator that will prefer placing tasks on agents that do not have a task with the same security groups.
 */
public class SecurityGroupSpreadingFitnessCalculator implements VMTaskFitnessCalculator {

    private static final double MATCHING_TASK_SCORE = 0.5;
    private static final double NO_MATCHING_TASK_SCORE = 1.0;

    @Override
    public String getName() {
        return "Security Group Spreading Fitness Calculator";
    }

    @Override
    public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        List<TaskRequest> allTasksOnAgent = getAllTasksOnAgent(targetVM);
        String currentTaskRequestJoinedSecurityGroupIds = FitnessCalculatorFunctions.getJoinedSecurityGroupIds(taskRequest);
        long matchingTaskCount = countMatchingTasks(allTasksOnAgent, taskOnAgent -> {
            String taskOnAgentJoinedSecurityGroupIds = FitnessCalculatorFunctions.getJoinedSecurityGroupIds(taskOnAgent);
            return currentTaskRequestJoinedSecurityGroupIds.equals(taskOnAgentJoinedSecurityGroupIds);
        });

        if (matchingTaskCount == 0) {
            return NO_MATCHING_TASK_SCORE;
        }

        double matchingTaskRatio = 1.0 / (double) matchingTaskCount;
        return matchingTaskRatio * MATCHING_TASK_SCORE;
    }
}
