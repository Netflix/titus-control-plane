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

package com.netflix.titus.master.clusteroperations;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.Tier;

public class ClusterOperationFunctions {

    public static TierAutoScalingConfiguration getTierConfiguration(Tier tier, ClusterOperationsConfiguration configuration) {
        switch (tier) {
            case Critical:
                return new TierAutoScalingConfiguration(
                        configuration.getCriticalPrimaryInstanceType(),
                        configuration.getCriticalScaleUpCoolDownMs(),
                        configuration.getCriticalScaleDownCoolDownMs(),
                        configuration.getCriticalMinIdle(),
                        configuration.getCriticalMaxIdle(),
                        configuration.getCriticalScaleUpAdjustingFactor(),
                        configuration.getCriticalTaskSloMs()
                );
            case Flex:
                return new TierAutoScalingConfiguration(
                        configuration.getFlexPrimaryInstanceType(),
                        configuration.getFlexScaleUpCoolDownMs(),
                        configuration.getFlexScaleDownCoolDownMs(),
                        configuration.getFlexMinIdle(),
                        configuration.getFlexMaxIdle(),
                        configuration.getFlexScaleUpAdjustingFactor(),
                        configuration.getFlexTaskSloMs()
                );
        }
        throw new IllegalArgumentException("Unknown Tier: " + tier);
    }

    public static boolean hasTimeElapsed(long start, long finish, long elapsed) {
        return finish - start >= elapsed;
    }

    public static int applyScalingFactor(int adjustingFactor, int scaleUpCount) {
        if (adjustingFactor <= 0) {
            return scaleUpCount;
        }
        return (int) Math.ceil(scaleUpCount / (double) scaleUpCount);
    }

    public static boolean canFit(ContainerResources containerResources, ResourceDimension resourceDimension) {
        return containerResources.getCpu() <= resourceDimension.getCpu() &&
                containerResources.getMemoryMB() <= resourceDimension.getMemoryMB() &&
                containerResources.getDiskMB() <= resourceDimension.getDiskMB() &&
                containerResources.getNetworkMbps() <= resourceDimension.getNetworkMbs();
    }

    public static Map<String, Long> getNumberOfTasksOnAgents(Collection<Task> tasks) {
        return tasks.stream()
                .collect(Collectors.groupingBy(
                        task -> task.getTaskContext().getOrDefault(TaskAttributes.TASK_ATTRIBUTES_AGENT_ID, "Unknown"),
                        Collectors.counting())
                );
    }
}
