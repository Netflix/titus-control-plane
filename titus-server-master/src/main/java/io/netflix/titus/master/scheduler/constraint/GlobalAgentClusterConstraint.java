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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.queues.QueuableTask;
import io.netflix.titus.api.model.Tier;
import io.netflix.titus.master.config.MasterConfiguration;
import io.netflix.titus.master.service.management.CapacityManagementConfiguration;
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
    private final Map<Integer, String[]> tierInstancesMap;
    private final MasterConfiguration config;
    private final String agentInstanceTypeAttrName;
    private final CapacityManagementConfiguration capacityManagementConfiguration;

    @Inject
    public GlobalAgentClusterConstraint(MasterConfiguration config, CapacityManagementConfiguration capacityManagementConfiguration) {
        this.capacityManagementConfiguration = capacityManagementConfiguration;
        tierInstancesMap = new HashMap<>();
        this.config = config;
        agentInstanceTypeAttrName = config.getInstanceTypeAttributeName();
    }

    @Override
    public String getName() {
        return GlobalAgentClusterConstraint.class.getSimpleName();
    }

    // Expected to be not called concurrently with evaluate().
    // Capacity management config can change dynamically. Setting up this configuration for our use involves a small
    // cost of setting a new map. We want to do that once for a scheduling iteration and refer to it for all invocations
    // of this constraint from within the iteration. Therefore, we expect this prepare() method to be called before the
    // start of the next scheduling iteration.
    @Override
    public void prepare() {
        try {
            Map<Integer, String[]> map = new HashMap<>();
            for (Tier t : Tier.values()) {
                final CapacityManagementConfiguration.TierConfig tierConfig =
                        capacityManagementConfiguration.getTiers().get(Integer.toString(t.ordinal()));
                map.put(t.ordinal(),
                        tierConfig == null || tierConfig.getInstanceTypes() == null || tierConfig.getInstanceTypes().length == 0 ?
                                new String[]{} :
                                tierConfig.getInstanceTypes()
                );
            }
            tierInstancesMap.clear();
            tierInstancesMap.putAll(map);
        } catch (Throwable t) {
            // In case there are problems getting tier config, leave tierInstancesMap as it was before
            logger.error("Unexpected error preparing global constraint: " + t.getMessage(), t);
        }
    }

    @Override
    public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        return config.getMultiAgentClusterPinningEnabled() && !tierInstancesMap.isEmpty() ?
                evaluateGpuAndCapacityTierPinning(taskRequest, targetVM) :
                evaluateGpuOnly(taskRequest, targetVM);
    }

    private Result evaluateGpuOnly(TaskRequest taskRequest, VirtualMachineCurrentState targetVM) {
        Double gpu = targetVM.getCurrAvailableResources().getScalarValue("gpu");
        if (gpu == null) {
            gpu = 0.0;
        }
        final boolean taskRequestsGpu = taskRequestsGpu(taskRequest);
        return taskRequestsGpu == gpu > 0.0 ?
                new Result(true, null) :
                new Result(
                        false,
                        taskRequestsGpu ?
                                "No GPU on agent" :
                                "Agent does not run non-GPU tasks"
                );
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
        final String[] instanceTypes = tierInstancesMap.get(qt.getQAttributes().getTierNumber());
        if (instanceTypes == null || instanceTypes.length == 0) {
            return new Result(true, null); // no pinning
        }
        final String agentInstTypeValue = getAgentAttrValue(targetVM, agentInstanceTypeAttrName);
        if (agentInstTypeValue == null) {
            return new Result(false, "No info for agent instance type attribute, " + agentInstanceTypeAttrName);
        }
        for (String it : instanceTypes) {
            if (agentInstTypeValue.equals(it)) {
                return new Result(true, null);
            }
        }
        return new Result(false, "Only runs on agent clusters: " + Arrays.toString(instanceTypes));
    }

    private String getAgentAttrValue(VirtualMachineCurrentState targetVM, String attrName) {
        Protos.Attribute attribute = targetVM.getCurrAvailableResources().getAttributeMap().get(attrName);
        return attribute == null ?
                null :
                attribute.getText().getValue();
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
