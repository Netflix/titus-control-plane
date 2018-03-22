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

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.VirtualMachineCurrentState;
import io.netflix.titus.api.agent.model.AgentInstanceGroup;
import io.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import io.netflix.titus.api.agent.service.AgentManagementService;
import io.netflix.titus.master.scheduler.SchedulerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.netflix.titus.master.scheduler.fitness.FitnessCalculatorFunctions.getAgentAttributeValue;

/**
 *
 */
@Singleton
public class AgentManagementFitnessCalculator implements VMTaskFitnessCalculator {

    private static final Logger logger = LoggerFactory.getLogger(AgentManagementFitnessCalculator.class);
    private static final double ACTIVE_INSTANCE_GROUP_SCORE = 1.0;
    private static final double PHASED_OUT_INSTANCE_GROUP_SCORE = 0.5;
    private static final double NOT_ACTIVE_INSTANCE_GROUP_SCORE = 0.01;

    private final SchedulerConfiguration schedulerConfiguration;
    private final AgentManagementService agentManagementService;

    @Inject
    public AgentManagementFitnessCalculator(SchedulerConfiguration schedulerConfiguration,
                                            AgentManagementService agentManagementService) {
        this.schedulerConfiguration = schedulerConfiguration;
        this.agentManagementService = agentManagementService;
    }

    @Override
    public String getName() {
        return "Agent Management Fitness Calculator";
    }

    @Override
    public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        String instanceGroupId = getAgentAttributeValue(targetVM, schedulerConfiguration.getInstanceGroupAttributeName());
        AgentInstanceGroup instanceGroup = null;
        try {
            instanceGroup = agentManagementService.getInstanceGroup(instanceGroupId);
        } catch (Exception e) {
            logger.debug("Ignoring instanceGroupId: {} because it was not found in agent management", instanceGroupId, e);
        }
        if (instanceGroup != null) {
            if (instanceGroup.getLifecycleStatus().getState() == InstanceGroupLifecycleState.Active) {
                return ACTIVE_INSTANCE_GROUP_SCORE;
            } else if (instanceGroup.getLifecycleStatus().getState() == InstanceGroupLifecycleState.PhasedOut) {
                return PHASED_OUT_INSTANCE_GROUP_SCORE;
            }
        }

        return NOT_ACTIVE_INSTANCE_GROUP_SCORE;
    }
}
