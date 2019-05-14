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

import java.util.Optional;

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.common.annotation.Experimental;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;
import com.netflix.titus.master.scheduler.SchedulerUtils;

/**
 * Experimental constraint such that workloads can prefer a specific machine type.
 */
@Experimental(deadline = "06/2019")
public class MachineTypeConstraint implements ConstraintEvaluator {
    public static final String NAME = "MachineTypeConstraint";

    private static final Result VALID = new Result(true, null);
    private static final Result MACHINE_DOES_NOT_EXIST = new Result(false, "The machine does not exist");
    private static final Result MACHINE_TYPE_DOES_NOT_MATCH = new Result(false, "The machine type does not match the specified machine type");
    private final SchedulerConfiguration configuration;
    private final AgentManagementService agentManagementService;
    private final String machineType;

    public MachineTypeConstraint(SchedulerConfiguration configuration, AgentManagementService agentManagementService, String machineType) {
        this.configuration = configuration;
        this.agentManagementService = agentManagementService;
        this.machineType = machineType;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        Optional<AgentInstance> instanceOpt = SchedulerUtils.findInstance(agentManagementService, configuration.getInstanceAttributeName(), targetVM);
        if (!instanceOpt.isPresent()) {
            return MACHINE_DOES_NOT_EXIST;
        }

        AgentInstance agentInstance = instanceOpt.get();
        String instanceInstanceType = agentInstance.getAttributes().getOrDefault(configuration.getMachineTypeAttributeName(), "");
        return instanceInstanceType.equalsIgnoreCase(machineType) ? VALID : MACHINE_TYPE_DOES_NOT_MATCH;
    }
}
