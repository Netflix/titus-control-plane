/*
 * Copyright 2020 Netflix, Inc.
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

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;

@Singleton
public class AgentContainerLimitSystemConstraint implements SystemConstraint {

    public static final String NAME = AgentContainerLimitSystemConstraint.class.getSimpleName();

    private static final Result VALID = new Result(true, "");
    private static final Result TOO_MANY_CONTAINERS = new Result(false, "Too many containers on the agent node");

    private final SchedulerConfiguration configuration;

    @Inject
    public AgentContainerLimitSystemConstraint(SchedulerConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        int total = targetVM.getRunningTasks().size() + targetVM.getTasksCurrentlyAssigned().size();
        int max = Math.max(1, configuration.getMaxTasksPerMachine());
        return total < max ? VALID : TOO_MANY_CONTAINERS;
    }

    public static boolean isAgentContainerLimitSystemConstraint(String reason) {
        return reason != null && TOO_MANY_CONTAINERS.getFailureReason().equals(reason);
    }
}
