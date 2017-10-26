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

import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.base.Strings;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.queues.QueuableTask;
import io.netflix.titus.api.agent.model.AgentInstanceGroup;
import io.netflix.titus.api.agent.model.monitor.AgentStatus;
import io.netflix.titus.api.agent.service.AgentManagementService;
import io.netflix.titus.api.agent.service.AgentStatusMonitor;
import io.netflix.titus.api.model.Tier;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.config.MasterConfiguration;
import io.netflix.titus.master.scheduler.SchedulerConfiguration;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private final AgentManagementService agentManagementService;

    private final String instanceGroupAttributeName;
    private final AgentStatusMonitor agentStatusMonitor;

    private final String instanceIdAttribute;

    @Inject
    public GlobalAgentClusterConstraint(MasterConfiguration configuration,
                                        SchedulerConfiguration schedulerConfiguration,
                                        AgentManagementService agentManagementService,
                                        AgentStatusMonitor agentStatusMonitor) {
        this.agentManagementService = agentManagementService;
        this.agentStatusMonitor = agentStatusMonitor;

        this.instanceIdAttribute = configuration.getAutoScalerMapHostnameAttributeName();
        this.instanceGroupAttributeName = schedulerConfiguration.getInstanceGroupAttributeName();
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
            status = agentStatusMonitor.getStatus(targetVM.getCurrAvailableResources().getAttributeMap().get(instanceIdAttribute).getText().getValue());
        } catch (Exception e) {
            logger.warn("Cannot resolve target VM health status", e);
            return Pair.of(false, "Unhealthy: Cannot find agent");
        }
        boolean healthy = status.getStatusCode() == AgentStatus.AgentStatusCode.Healthy;
        return Pair.of(healthy, status.getStatusCode().name() + ": " + status.getDescription());
    }

    private Result evaluateGpuAndCapacityTierPinning(TaskRequest taskRequest, VirtualMachineCurrentState targetVM) {
        final boolean taskRequestsGpu = taskRequestsGpu(taskRequest);
        final Double GPU = targetVM.getCurrAvailableResources().getScalarValue("gpu");
        final double gpu = GPU == null ? 0.0 : GPU;
        if (taskRequestsGpu != gpu > 0.0) {
            // GPU mismatch
            return new Result(false,
                    taskRequestsGpu ?
                            "No GPU on agent" :
                            "Agent does not run non-GPU tasks"
            );
        }
        // Since we moved to using Fenzo queues, we know the task request will be of this type.
        QueuableTask qt = (QueuableTask) taskRequest;
        Tier tier = Tier.Flex;
        if (qt.getQAttributes().getTierNumber() == 0) {
            tier = Tier.Critical;
        }

        String instanceGroupId = getAgentAttributeValue(targetVM, instanceGroupAttributeName);
        if (Strings.isNullOrEmpty(instanceGroupId)) {
            return new Result(false, "No info for agent instance type attribute: " + instanceGroupAttributeName);
        }
        try {
            AgentInstanceGroup instanceGroup = agentManagementService.getInstanceGroup(instanceGroupId);
            if (instanceGroup.getTier() == tier) {
                return new Result(true, null);
            }
        } catch (Exception ignored) {
        }

        return new Result(false, "Only runs on tier: " + tier.name());
    }

    private String getAgentAttributeValue(VirtualMachineCurrentState targetVM, String attributeName) {
        Protos.Attribute attribute = targetVM.getCurrAvailableResources().getAttributeMap().get(attributeName);
        return Strings.nullToEmpty(attribute.getText().getValue());
    }

    private boolean taskRequestsGpu(TaskRequest taskRequest) {
        Map<String, Double> scalars = taskRequest.getScalarRequests();
        if (scalars != null && !scalars.isEmpty()) {
            final Double gpu = scalars.get("gpu");
            if (gpu != null && gpu >= 1.0) {
                return true;
            }
        }
        return false;
    }
}