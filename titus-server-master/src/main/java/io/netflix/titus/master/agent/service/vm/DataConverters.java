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

package io.netflix.titus.master.agent.service.vm;

import java.util.Collections;

import io.netflix.titus.api.agent.model.AgentInstance;
import io.netflix.titus.api.agent.model.AgentInstanceGroup;
import io.netflix.titus.api.agent.model.AutoScaleRule;
import io.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import io.netflix.titus.api.agent.model.InstanceGroupLifecycleStatus;
import io.netflix.titus.api.agent.model.InstanceLifecycleState;
import io.netflix.titus.api.agent.model.InstanceLifecycleStatus;
import io.netflix.titus.api.agent.model.InstanceOverrideStatus;
import io.netflix.titus.api.connector.cloud.Instance;
import io.netflix.titus.api.connector.cloud.InstanceGroup;
import io.netflix.titus.api.model.ResourceDimension;
import io.netflix.titus.api.model.Tier;
import io.netflix.titus.common.util.Evaluators;

class DataConverters {

    static AgentInstanceGroup toAgentInstanceGroup(InstanceGroup instanceGroup,
                                                   ResourceDimension instanceResourceDimension,
                                                   AutoScaleRule defaultAutoScaleRule) {
        long now = System.currentTimeMillis();
        String instanceType = instanceGroup.getAttributes().getOrDefault(VmServersCache.ATTR_INSTANCE_TYPE, "unknown");

        return AgentInstanceGroup.newBuilder()
                .withId(instanceGroup.getId())
                .withLifecycleStatus(InstanceGroupLifecycleStatus.newBuilder()
                        .withState(InstanceGroupLifecycleState.Inactive)
                        .withDetail("New instance group discovered")
                        .withTimestamp(now)
                        .build()
                )
                .withTier(Tier.Flex)
                .withInstanceType(instanceType)
                .withResourceDimension(instanceResourceDimension)
                .withAutoScaleRule(defaultAutoScaleRule)
                .withMin(instanceGroup.getMin())
                .withDesired(instanceGroup.getDesired())
                .withMax(instanceGroup.getMax())
                .withLaunchTimestamp(now)
                .withAttributes(Collections.emptyMap())
                .withCurrent(instanceGroup.getInstanceIds().size())
                .withIsLaunchEnabled(!instanceGroup.isLaunchSuspended())
                .withIsTerminateEnabled(!instanceGroup.isTerminateSuspended())
                .withTimestamp(now)
                .build();
    }

    static AgentInstanceGroup updateAgentInstanceGroup(AgentInstanceGroup original, InstanceGroup instanceGroup) {
        long now = System.currentTimeMillis();

        return original.toBuilder()
                .withMin(instanceGroup.getMin())
                .withDesired(instanceGroup.getDesired())
                .withMax(instanceGroup.getMax())
                .withCurrent(instanceGroup.getInstanceIds().size())
                .withIsLaunchEnabled(!instanceGroup.isLaunchSuspended())
                .withIsTerminateEnabled(!instanceGroup.isTerminateSuspended())
                .withTimestamp(now)
                .build();
    }

    static AgentInstance toAgentInstance(Instance instance) {
        AgentInstance.Builder builder = AgentInstance.newBuilder();
        return updateInstanceAttributes(builder, instance)
                .withId(instance.getId())
                .withIpAddress(instance.getIpAddress())
                .withHostname(instance.getHostname())
                .withDeploymentStatus(toDeploymentStatus(instance))
                .withOverrideStatus(InstanceOverrideStatus.none())
                .withAttributes(instance.getAttributes())
                .withTimestamp(System.currentTimeMillis())
                .build();
    }

    static AgentInstance updateAgentInstance(AgentInstance original, Instance instance) {
        AgentInstance.Builder builder = original.toBuilder();
        return updateInstanceAttributes(builder, instance).withDeploymentStatus(toDeploymentStatus(instance)).build();
    }

    private static AgentInstance.Builder updateInstanceAttributes(AgentInstance.Builder builder, Instance instance) {
        return builder.withIpAddress(Evaluators.getOrDefault(instance.getIpAddress(), "0.0.0.0"))
                .withInstanceGroupId(Evaluators.getOrDefault(instance.getInstanceGroupId(), "detached"))
                .withHostname(Evaluators.getOrDefault(instance.getHostname(), "0_0_0_0"))
                .withAttributes(instance.getAttributes())
                .withTimestamp(System.currentTimeMillis());
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
