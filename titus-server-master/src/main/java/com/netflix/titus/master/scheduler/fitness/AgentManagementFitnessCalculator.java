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

package com.netflix.titus.master.scheduler.fitness;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.master.scheduler.AgentQualityTracker;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@Singleton
public class AgentManagementFitnessCalculator implements VMTaskFitnessCalculator {

    public static final String NAME = "AgentManagementFitnessCalculator";

    private static final Logger logger = LoggerFactory.getLogger(AgentManagementFitnessCalculator.class);

    private static final double ACTIVE_INSTANCE_GROUP_SCORE = 1.0;
    private static final double PREFER_NO_PLACEMENT_SCORE = 0.01;
    private static final double DEFAULT_SCORE = 0.01;

    private static final double QUALITY_OF_UNKNOWN_AGENT = 0.5;

    private final SchedulerConfiguration schedulerConfiguration;
    private final AgentManagementService agentManagementService;
    private final AgentQualityTracker agentQualityTracker;

    @Inject
    public AgentManagementFitnessCalculator(SchedulerConfiguration schedulerConfiguration,
                                            AgentManagementService agentManagementService,
                                            AgentQualityTracker agentQualityTracker) {
        this.schedulerConfiguration = schedulerConfiguration;
        this.agentManagementService = agentManagementService;
        this.agentQualityTracker = agentQualityTracker;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        String instanceGroupId = FitnessCalculatorFunctions.getAgentAttributeValue(targetVM, schedulerConfiguration.getInstanceGroupAttributeName());
        AgentInstanceGroup instanceGroup = null;
        try {
            instanceGroup = agentManagementService.getInstanceGroup(instanceGroupId);
        } catch (Exception e) {
            logger.debug("Ignoring instanceGroupId: {} because it was not found in agent management", instanceGroupId, e);
        }
        if (instanceGroup != null) {
            double quality = Math.min(1.0, agentQualityTracker.qualityOf(targetVM.getHostname()));
            if (quality <= 0) {
                // If we have no information about the agent, we have to assume something.
                quality = QUALITY_OF_UNKNOWN_AGENT;
            }

            if (instanceGroup.getLifecycleStatus().getState() == InstanceGroupLifecycleState.Active) {
                return quality * ACTIVE_INSTANCE_GROUP_SCORE;
            } else if (instanceGroup.getLifecycleStatus().getState() == InstanceGroupLifecycleState.PhasedOut) {
                return quality * PREFER_NO_PLACEMENT_SCORE;
            }
        }

        return DEFAULT_SCORE;
    }
}
