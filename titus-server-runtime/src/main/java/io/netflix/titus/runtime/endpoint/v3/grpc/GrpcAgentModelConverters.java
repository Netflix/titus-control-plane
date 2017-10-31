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

package io.netflix.titus.runtime.endpoint.v3.grpc;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.netflix.titus.grpc.protogen.AgentChangeEvent;
import com.netflix.titus.grpc.protogen.HealthStatus;
import io.netflix.titus.api.agent.model.AgentInstance;
import io.netflix.titus.api.agent.model.AgentInstanceGroup;
import io.netflix.titus.api.agent.model.AutoScaleRule;
import io.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import io.netflix.titus.api.agent.model.InstanceGroupLifecycleStatus;
import io.netflix.titus.api.agent.model.InstanceLifecycleState;
import io.netflix.titus.api.agent.model.InstanceLifecycleStatus;
import io.netflix.titus.api.agent.model.InstanceOverrideState;
import io.netflix.titus.api.agent.model.InstanceOverrideStatus;
import io.netflix.titus.api.agent.model.event.AgentEvent;
import io.netflix.titus.api.agent.model.event.AgentInstanceGroupRemovedEvent;
import io.netflix.titus.api.agent.model.event.AgentInstanceGroupUpdateEvent;
import io.netflix.titus.api.agent.model.event.AgentInstanceRemovedEvent;
import io.netflix.titus.api.agent.model.event.AgentInstanceUpdateEvent;
import io.netflix.titus.api.agent.model.event.AgentSnapshotEndEvent;
import io.netflix.titus.api.agent.model.monitor.AgentStatus;
import io.netflix.titus.api.agent.service.AgentStatusMonitor;
import io.netflix.titus.api.model.ResourceDimension;
import io.netflix.titus.api.model.Tier;

public class GrpcAgentModelConverters {

    public static com.netflix.titus.grpc.protogen.AgentInstanceGroup toGrpcAgentInstanceGroup(AgentInstanceGroup coreAgentGroup) {
        return com.netflix.titus.grpc.protogen.AgentInstanceGroup.newBuilder()
                .setId(coreAgentGroup.getId())
                .setInstanceType(coreAgentGroup.getInstanceType())
                .setInstanceResources(toGrpcInstanceResources(coreAgentGroup.getResourceDimension()))
                .setTier(toGrpcTier(coreAgentGroup.getTier()))
                .setMin(coreAgentGroup.getMin())
                .setDesired(coreAgentGroup.getDesired())
                .setCurrent(coreAgentGroup.getCurrent())
                .setMax(coreAgentGroup.getMax())
                .setIsLaunchEnabled(coreAgentGroup.isLaunchEnabled())
                .setIsTerminateEnabled(coreAgentGroup.isTerminateEnabled())
                .setAutoScaleRule(toGrpcAutoScaleRule(coreAgentGroup.getAutoScaleRule()))
                .setLifecycleStatus(toGrpcLifecycleStatus(coreAgentGroup.getLifecycleStatus()))
                .setLaunchTimestamp(coreAgentGroup.getLaunchTimestamp())
                .putAllAttributes(coreAgentGroup.getAttributes())
                .build();
    }

    private static com.netflix.titus.grpc.protogen.ResourceDimension toGrpcInstanceResources(ResourceDimension resourceDimension) {
        return com.netflix.titus.grpc.protogen.ResourceDimension.newBuilder()
                .setCpu((int) resourceDimension.getCpu())
                .setGpu(resourceDimension.getGpu())
                .setMemoryMB(resourceDimension.getMemoryMB())
                .setDiskMB(resourceDimension.getDiskMB())
                .setNetworkMbps(resourceDimension.getNetworkMbs())
                .build();
    }

    public static com.netflix.titus.grpc.protogen.AgentInstance toGrpcAgentInstance(AgentInstance coreAgentInstance, AgentStatus agentStatus) {
        com.netflix.titus.grpc.protogen.HealthStatus healthStatus = toGrpcHealthStatus(agentStatus);

        return com.netflix.titus.grpc.protogen.AgentInstance.newBuilder()
                .setId(coreAgentInstance.getId())
                .setInstanceGroupId(coreAgentInstance.getInstanceGroupId())
                .setIpAddress(coreAgentInstance.getIpAddress())
                .setHostname(coreAgentInstance.getHostname())
                .setLifecycleStatus(toGrpcDeploymentStatus(coreAgentInstance.getLifecycleStatus()))
                .setOverrideStatus(toGrpcOverrideStatus(coreAgentInstance.getOverrideStatus()))
                .setHealthStatus(healthStatus)
                .putAllAttributes(coreAgentInstance.getAttributes())
                .build();
    }

    private static com.netflix.titus.grpc.protogen.AutoScaleRule toGrpcAutoScaleRule(AutoScaleRule autoScaleRule) {
        return com.netflix.titus.grpc.protogen.AutoScaleRule.newBuilder()
                .setMin(autoScaleRule.getMin())
                .setMax(autoScaleRule.getMax())
                .setMinIdleToKeep(autoScaleRule.getMinIdleToKeep())
                .setMaxIdleToKeep(autoScaleRule.getMaxIdleToKeep())
                .setCoolDownSec(autoScaleRule.getCoolDownSec())
                .setPriority(autoScaleRule.getPriority())
                .setShortfallAdjustingFactor(autoScaleRule.getShortfallAdjustingFactor())
                .build();
    }

    public static AutoScaleRule toCoreAutoScaleRule(com.netflix.titus.grpc.protogen.AutoScaleRule grpcAutoScaleRule) {
        return AutoScaleRule.newBuilder()
                .withMin(grpcAutoScaleRule.getMin())
                .withMax(grpcAutoScaleRule.getMax())
                .withMinIdleToKeep(grpcAutoScaleRule.getMinIdleToKeep())
                .withMaxIdleToKeep(grpcAutoScaleRule.getMaxIdleToKeep())
                .withCoolDownSec((int) grpcAutoScaleRule.getCoolDownSec())
                .withPriority(grpcAutoScaleRule.getPriority())
                .withShortfallAdjustingFactor(grpcAutoScaleRule.getShortfallAdjustingFactor())
                .build();
    }

    public static InstanceGroupLifecycleState toCoreLifecycleState(com.netflix.titus.grpc.protogen.InstanceGroupLifecycleState lifecycleState) {
        switch (lifecycleState) {
            case Inactive:
                return InstanceGroupLifecycleState.Inactive;
            case Active:
                return InstanceGroupLifecycleState.Active;
            case PhasedOut:
                return InstanceGroupLifecycleState.PhasedOut;
            case Removable:
                return InstanceGroupLifecycleState.Removable;
        }
        return InstanceGroupLifecycleState.Inactive;
    }

    public static InstanceOverrideState toCoreOverrideState(com.netflix.titus.grpc.protogen.InstanceOverrideState overrideState) {
        switch (overrideState) {
            case None:
                return InstanceOverrideState.None;
            case Quarantined:
                return InstanceOverrideState.Quarantined;
        }
        return InstanceOverrideState.None;
    }

    private static com.netflix.titus.grpc.protogen.InstanceGroupLifecycleState toGrpcLifecycleState(InstanceGroupLifecycleState state) {
        switch (state) {
            case Inactive:
                return com.netflix.titus.grpc.protogen.InstanceGroupLifecycleState.Inactive;
            case Active:
                return com.netflix.titus.grpc.protogen.InstanceGroupLifecycleState.Active;
            case PhasedOut:
                return com.netflix.titus.grpc.protogen.InstanceGroupLifecycleState.PhasedOut;
            case Removable:
                return com.netflix.titus.grpc.protogen.InstanceGroupLifecycleState.Removable;
        }
        return com.netflix.titus.grpc.protogen.InstanceGroupLifecycleState.UNRECOGNIZED;
    }

    public static com.netflix.titus.grpc.protogen.InstanceGroupLifecycleStatus toGrpcLifecycleStatus(InstanceGroupLifecycleStatus instanceGroupLifecycleStatus) {
        return com.netflix.titus.grpc.protogen.InstanceGroupLifecycleStatus.newBuilder()
                .setState(toGrpcLifecycleState(instanceGroupLifecycleStatus.getState()))
                .setDetail(instanceGroupLifecycleStatus.getDetail())
                .setTimestamp(instanceGroupLifecycleStatus.getTimestamp())
                .build();
    }

    public static com.netflix.titus.grpc.protogen.InstanceLifecycleState toGrpcDeploymentState(InstanceLifecycleState state) {
        switch (state) {
            case Launching:
                return com.netflix.titus.grpc.protogen.InstanceLifecycleState.StartInitiated;
            case Started:
                return com.netflix.titus.grpc.protogen.InstanceLifecycleState.Started;
            case KillInitiated:
                return com.netflix.titus.grpc.protogen.InstanceLifecycleState.KillInitiated;
            case Stopped:
                return com.netflix.titus.grpc.protogen.InstanceLifecycleState.Stopped;
        }
        return com.netflix.titus.grpc.protogen.InstanceLifecycleState.UNRECOGNIZED;
    }

    public static com.netflix.titus.grpc.protogen.InstanceLifecycleStatus toGrpcDeploymentStatus(InstanceLifecycleStatus instanceLifecycleStatus) {
        return com.netflix.titus.grpc.protogen.InstanceLifecycleStatus.newBuilder()
                .setState(toGrpcDeploymentState(instanceLifecycleStatus.getState()))
                .setLaunchTimestamp(instanceLifecycleStatus.getLaunchTimestamp())
                .build();
    }

    public static com.netflix.titus.grpc.protogen.InstanceOverrideState toGrpcOverrideState(InstanceOverrideState state) {
        switch (state) {
            case None:
                return com.netflix.titus.grpc.protogen.InstanceOverrideState.None;
            case Quarantined:
                return com.netflix.titus.grpc.protogen.InstanceOverrideState.Quarantined;
        }
        return com.netflix.titus.grpc.protogen.InstanceOverrideState.UNRECOGNIZED;
    }

    public static com.netflix.titus.grpc.protogen.InstanceOverrideStatus toGrpcOverrideStatus(InstanceOverrideStatus instanceOverrideStatus) {
        return com.netflix.titus.grpc.protogen.InstanceOverrideStatus.newBuilder()
                .setState(toGrpcOverrideState(instanceOverrideStatus.getState()))
                .setDetail(instanceOverrideStatus.getDetail())
                .setTimestamp(instanceOverrideStatus.getTimestamp())
                .build();
    }

    public static com.netflix.titus.grpc.protogen.HealthState toGrpcHealthState(AgentStatus.AgentStatusCode agentStatusCode) {
        switch (agentStatusCode) {
            case Healthy:
                return com.netflix.titus.grpc.protogen.HealthState.Healthy;
            case Unhealthy:
                return com.netflix.titus.grpc.protogen.HealthState.Unhealthy;
        }
        return com.netflix.titus.grpc.protogen.HealthState.UNRECOGNIZED;
    }

    public static com.netflix.titus.grpc.protogen.HealthStatus toGrpcHealthStatus(AgentStatus agentStatus) {
        HealthStatus.Builder builder = HealthStatus.newBuilder()
                .setSourceId(agentStatus.getSourceId())
                .setState(toGrpcHealthState(agentStatus.getStatusCode()))
                .setDetail(agentStatus.getDescription())
                .setTimestamp(agentStatus.getEmitTime());

        if (!agentStatus.getComponents().isEmpty()) {
            List<HealthStatus> components = agentStatus.getComponents().stream()
                    .map(GrpcAgentModelConverters::toGrpcHealthStatus)
                    .collect(Collectors.toList());
            builder.addAllComponents(components);
        }

        return builder.build();
    }

    public static Tier toCoreTier(com.netflix.titus.grpc.protogen.Tier grpcTier) {
        switch (grpcTier) {
            case Flex:
                return Tier.Flex;
            case Critical:
                return Tier.Critical;
        }
        return Tier.Flex; // Default to flex
    }

    public static com.netflix.titus.grpc.protogen.Tier toGrpcTier(Tier tier) {
        switch (tier) {
            case Critical:
                return com.netflix.titus.grpc.protogen.Tier.Critical;
            case Flex:
                return com.netflix.titus.grpc.protogen.Tier.Flex;
        }
        return com.netflix.titus.grpc.protogen.Tier.UNRECOGNIZED;
    }

    public static Optional<AgentChangeEvent> toGrpcEvent(AgentEvent agentEvent, AgentStatusMonitor agentStatusMonitor) {
        AgentChangeEvent event = null;

        if (agentEvent instanceof AgentInstanceGroupUpdateEvent) {
            com.netflix.titus.grpc.protogen.AgentInstanceGroup grpcInstanceGroup =
                    toGrpcAgentInstanceGroup(((AgentInstanceGroupUpdateEvent) agentEvent).getAgentInstanceGroup());

            event = AgentChangeEvent.newBuilder()
                    .setInstanceGroupUpdate(AgentChangeEvent.InstanceGroupUpdate.newBuilder().setInstanceGroup(grpcInstanceGroup))
                    .build();
        } else if (agentEvent instanceof AgentInstanceGroupRemovedEvent) {
            String instanceGroupId = ((AgentInstanceGroupRemovedEvent) agentEvent).getInstanceGroupId();
            event = AgentChangeEvent.newBuilder()
                    .setInstanceGroupRemoved(AgentChangeEvent.InstanceGroupRemoved.newBuilder().setInstanceGroupId(instanceGroupId))
                    .build();
        } else if (agentEvent instanceof AgentInstanceUpdateEvent) {
            AgentInstance instance = ((AgentInstanceUpdateEvent) agentEvent).getAgentInstance();
            AgentStatus status = agentStatusMonitor.getStatus(instance.getId());
            com.netflix.titus.grpc.protogen.AgentInstance agentInstance = toGrpcAgentInstance(instance, status);

            event = AgentChangeEvent.newBuilder()
                    .setAgentInstanceUpdate(AgentChangeEvent.InstanceUpdate.newBuilder().setInstance(agentInstance))
                    .build();
        } else if (agentEvent instanceof AgentInstanceRemovedEvent) {
            String instanceId = ((AgentInstanceRemovedEvent) agentEvent).getAgentInstanceId();
            event = AgentChangeEvent.newBuilder()
                    .setAgentInstanceRemoved(AgentChangeEvent.InstanceRemoved.newBuilder().setInstanceId(instanceId))
                    .build();
        } else if (agentEvent instanceof AgentSnapshotEndEvent) {
            event = AgentChangeEvent.newBuilder().setSnapshotEnd(AgentChangeEvent.SnapshotEnd.getDefaultInstance()).build();
        }

        return Optional.ofNullable(event);
    }
}
