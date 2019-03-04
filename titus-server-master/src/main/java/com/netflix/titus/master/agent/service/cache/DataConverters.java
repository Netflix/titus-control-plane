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

package com.netflix.titus.master.agent.service.cache;

import java.util.Collections;
import java.util.Map;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleStatus;
import com.netflix.titus.api.agent.model.InstanceLifecycleState;
import com.netflix.titus.api.agent.model.InstanceLifecycleStatus;
import com.netflix.titus.api.connector.cloud.Instance;
import com.netflix.titus.api.connector.cloud.InstanceGroup;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.master.agent.store.AgentStoreReaper;

class DataConverters {

    static AgentInstanceGroup toAgentInstanceGroup(InstanceGroup instanceGroup,
                                                   ResourceDimension instanceResourceDimension) {
        long now = System.currentTimeMillis();
        return AgentInstanceGroup.newBuilder()
                .withId(instanceGroup.getId())
                .withLifecycleStatus(InstanceGroupLifecycleStatus.newBuilder()
                        .withState(InstanceGroupLifecycleState.Inactive)
                        .withDetail("New instance group discovered")
                        .withTimestamp(now)
                        .build()
                )
                .withTier(Tier.Flex)
                .withInstanceType(instanceGroup.getInstanceType())
                .withResourceDimension(instanceResourceDimension)
                .withMin(instanceGroup.getMin())
                .withDesired(instanceGroup.getDesired())
                .withMax(instanceGroup.getMax())
                .withLaunchTimestamp(now)
                .withAttributes(instanceGroup.getAttributes())
                .withCurrent(instanceGroup.getInstanceIds().size())
                .withIsLaunchEnabled(!instanceGroup.isLaunchSuspended())
                .withIsTerminateEnabled(!instanceGroup.isTerminateSuspended())
                .withTimestamp(now)
                .build();
    }

    static AgentInstanceGroup updateAgentInstanceGroup(AgentInstanceGroup original, InstanceGroup instanceGroup) {
        long now = System.currentTimeMillis();
        Map<String, String> updatedAttributes = CollectionsExt.merge(original.getAttributes(), instanceGroup.getAttributes());
        updatedAttributes.remove(AgentStoreReaper.INSTANCE_GROUP_REMOVED_ATTRIBUTE);
        return original.toBuilder()
                .withMin(instanceGroup.getMin())
                .withDesired(instanceGroup.getDesired())
                .withMax(instanceGroup.getMax())
                .withCurrent(instanceGroup.getInstanceIds().size())
                .withIsLaunchEnabled(!instanceGroup.isLaunchSuspended())
                .withIsTerminateEnabled(!instanceGroup.isTerminateSuspended())
                .withTimestamp(now)
                .withAttributes(updatedAttributes)
                .build();
    }

    static AgentInstance toAgentInstance(Instance instance) {
        AgentInstance.Builder builder = AgentInstance.newBuilder();
        return updateInstanceAttributes(builder, instance, Collections.emptyMap())
                .withId(instance.getId())
                .withIpAddress(instance.getIpAddress())
                .withHostname(instance.getHostname())
                .withDeploymentStatus(toDeploymentStatus(instance))
                .withAttributes(instance.getAttributes())
                .build();
    }

    static AgentInstance updateAgentInstance(AgentInstance original, Instance instance) {
        AgentInstance.Builder builder = original.toBuilder();
        return updateInstanceAttributes(builder, instance, original.getAttributes()).withDeploymentStatus(toDeploymentStatus(instance)).build();
    }

    private static AgentInstance.Builder updateInstanceAttributes(AgentInstance.Builder builder, Instance instance, Map<String, String> originalAttributes) {
        Map<String, String> updatedAttributes = CollectionsExt.merge(originalAttributes, instance.getAttributes());
        return builder.withIpAddress(Evaluators.getOrDefault(instance.getIpAddress(), "0.0.0.0"))
                .withInstanceGroupId(Evaluators.getOrDefault(instance.getInstanceGroupId(), "detached"))
                .withHostname(Evaluators.getOrDefault(instance.getHostname(), "0_0_0_0"))
                .withAttributes(updatedAttributes);
    }

    static InstanceLifecycleStatus toDeploymentStatus(Instance instance) {
        InstanceLifecycleStatus.Builder deploymentStatusBuilder = InstanceLifecycleStatus.newBuilder();
        switch (instance.getInstanceState()) {
            case Starting:
                deploymentStatusBuilder.withState(InstanceLifecycleState.Launching);
                break;
            case Running:
            case Terminating:
            case Stopping:
                deploymentStatusBuilder.withState(InstanceLifecycleState.Started);
                break;
            case Stopped:
            case Terminated:
                deploymentStatusBuilder.withState(InstanceLifecycleState.Stopped);
                break;
            case Unknown:
            default:
                deploymentStatusBuilder.withState(InstanceLifecycleState.Unknown);
        }
        deploymentStatusBuilder.withLaunchTimestamp(instance.getLaunchTime());
        return deploymentStatusBuilder.build();
    }
}
