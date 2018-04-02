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

import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.base.Strings;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.queues.QueuableTask;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.monitor.AgentStatus;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.agent.service.AgentStatusMonitor;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;
import com.netflix.titus.master.scheduler.SchedulerUtils;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.master.scheduler.SchedulerUtils.getAttributeValue;

/**
 * A constraint to support global rules on picking agent clusters for jobs.
 * This constraint does two things:<br>
 * - GPU hard affinity (only jobs requesting GPU get resources from GPU agents)<br>
 * - match agents based on capacity management configuration for the task's capacity tier<br>
 * <p>
 * If there are no capacity management tiers defined, or if there are no instance types defined for a tier,
 * this constraint only performs the GPU hard affinity.
 */
@Singleton
public class GlobalAgentClusterConstraint implements GlobalConstraintEvaluator {

    private static final Logger logger = LoggerFactory.getLogger(GlobalAgentClusterConstraint.class);

    private final SchedulerConfiguration schedulerConfiguration;
    private final AgentManagementService agentManagementService;
    private final AgentStatusMonitor agentStatusMonitor;

    @Inject
    public GlobalAgentClusterConstraint(SchedulerConfiguration schedulerConfiguration,
                                        AgentManagementService agentManagementService,
                                        AgentStatusMonitor agentStatusMonitor) {
        this.schedulerConfiguration = schedulerConfiguration;
        this.agentManagementService = agentManagementService;
        this.agentStatusMonitor = agentStatusMonitor;
    }

    @Override
    public String getName() {
        return GlobalAgentClusterConstraint.class.getSimpleName();
    }

    @Override
    public void prepare() {
    }

    @Override
    public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        Pair<Boolean, String> health = evaluateHealthy(targetVM);
        if (!health.getLeft()) {
            return new Result(false, health.getRight());
        }
        return evaluateGpuAndCapacityTierPinning(taskRequest, targetVM);
    }

    private Pair<Boolean, String> evaluateHealthy(VirtualMachineCurrentState targetVM) {
        AgentStatus status;
        try {
            status = agentStatusMonitor.getStatus(getAttributeValue(targetVM, schedulerConfiguration.getInstanceAttributeName()));
        } catch (Exception e) {
            logger.debug("Cannot evaluate health of agent: ", e);
            return Pair.of(false, "Unhealthy: Cannot find agent");
        }
        boolean healthy = status.getStatusCode() == AgentStatus.AgentStatusCode.Healthy;
        return Pair.of(healthy, status.getStatusCode().name() + ": " + status.getDescription());
    }

    private Result evaluateGpuAndCapacityTierPinning(TaskRequest taskRequest, VirtualMachineCurrentState targetVM) {
        // Since we moved to using Fenzo queues, we know the task request will be of this type.
        Tier tier = getTier((QueuableTask) taskRequest);

        String instanceGroupAttributeName = schedulerConfiguration.getInstanceGroupAttributeName();
        String instanceGroupId = getAttributeValue(targetVM, instanceGroupAttributeName);
        if (Strings.isNullOrEmpty(instanceGroupId)) {
            return new Result(false, "No info for agent instance type attribute: " + instanceGroupAttributeName);
        }
        AgentInstanceGroup instanceGroup;
        try {
            instanceGroup = agentManagementService.getInstanceGroup(instanceGroupId);
        } catch (Exception ignored) {
            return new Result(false, "Instance group type not registered with the agent management subsystem: " + instanceGroupId);
        }

        // Check tier
        if (instanceGroup.getTier() != tier) {
            return new Result(false, "Only runs on tier: " + tier.name());
        }

        // Check GPU
        boolean gpuTask = taskRequestsGpu(taskRequest);
        boolean gpuAgent = instanceGroup.getResourceDimension().getGpu() > 0;

        if (gpuTask && !gpuAgent) {
            return new Result(false, "No GPU on agent");
        }
        if (!gpuTask && gpuAgent) {
            return new Result(false, "Agent does not run non-GPU tasks");
        }

        return new Result(true, null);
    }

    private Tier getTier(QueuableTask qt) {
        Tier tier = Tier.Flex;
        if (qt.getQAttributes().getTierNumber() == 0) {
            tier = Tier.Critical;
        }
        return tier;
    }

    private boolean taskRequestsGpu(TaskRequest taskRequest) {
        Map<String, Double> scalars = taskRequest.getScalarRequests();
        if (scalars != null && !scalars.isEmpty()) {
            final Double gpu = scalars.get("gpu");
            return gpu != null && gpu >= 1.0;
        }
        return false;
    }
}