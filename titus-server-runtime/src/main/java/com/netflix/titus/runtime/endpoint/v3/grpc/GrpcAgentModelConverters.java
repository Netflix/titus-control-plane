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

package com.netflix.titus.runtime.endpoint.v3.grpc;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleStatus;
import com.netflix.titus.api.agent.model.InstanceLifecycleState;
import com.netflix.titus.api.agent.model.InstanceLifecycleStatus;
import com.netflix.titus.api.agent.model.event.AgentEvent;
import com.netflix.titus.api.agent.model.event.AgentInstanceGroupRemovedEvent;
import com.netflix.titus.api.agent.model.event.AgentInstanceGroupUpdateEvent;
import com.netflix.titus.api.agent.model.event.AgentInstanceRemovedEvent;
import com.netflix.titus.api.agent.model.event.AgentInstanceUpdateEvent;
import com.netflix.titus.api.agent.model.event.AgentSnapshotEndEvent;
import com.netflix.titus.api.agent.model.monitor.AgentStatus;
import com.netflix.titus.api.agent.service.AgentStatusMonitor;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.grpc.protogen.AgentChangeEvent;
import com.netflix.titus.grpc.protogen.HealthState;
import com.netflix.titus.grpc.protogen.HealthStatus;

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
                .setLifecycleStatus(toGrpcLifecycleStatus(coreAgentGroup.getLifecycleStatus()))
                .setLaunchTimestamp(coreAgentGroup.getLaunchTimestamp())
                .putAllAttributes(coreAgentGroup.getAttributes())
                .build();
    }

    private static com.netflix.titus.grpc.protogen.ResourceDimension toGrpcInstanceResources(ResourceDimension resourceDimension) {
        return com.netflix.titus.grpc.protogen.ResourceDimension.newBuilder()
                .setCpu((int) resourceDimension.getCpu())
                .setGpu((int) resourceDimension.getGpu())
                .setMemoryMB((int) resourceDimension.getMemoryMB())
                .setDiskMB((int) resourceDimension.getDiskMB())
                .setNetworkMbps((int) resourceDimension.getNetworkMbs())
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
                .setHealthStatus(healthStatus)
                .putAllAttributes(coreAgentInstance.getAttributes())
                .build();
    }

    public static AgentInstanceGroup toCoreAgentInstanceGroup(com.netflix.titus.grpc.protogen.AgentInstanceGroup grpcAgentInstanceGroup) {
        return AgentInstanceGroup.newBuilder()
                .withId(grpcAgentInstanceGroup.getId())
                .withResourceDimension(toCoreResourceDimension(grpcAgentInstanceGroup.getInstanceResources()))
                .withInstanceType(grpcAgentInstanceGroup.getInstanceType())
                .withTier(toCoreTier(grpcAgentInstanceGroup.getTier()))
                .withMin(grpcAgentInstanceGroup.getMin())
                .withDesired(grpcAgentInstanceGroup.getDesired())
                .withCurrent(grpcAgentInstanceGroup.getCurrent())
                .withMax(grpcAgentInstanceGroup.getMax())
                .withLifecycleStatus(toCoreLifecycleStatus(grpcAgentInstanceGroup.getLifecycleStatus()))
                .withIsLaunchEnabled(grpcAgentInstanceGroup.getIsLaunchEnabled())
                .withIsTerminateEnabled(grpcAgentInstanceGroup.getIsTerminateEnabled())
                .withLaunchTimestamp(grpcAgentInstanceGroup.getLaunchTimestamp())
                .withAttributes(grpcAgentInstanceGroup.getAttributesMap())
                .build();
    }

    public static AgentInstance toCoreAgentInstance(com.netflix.titus.grpc.protogen.AgentInstance grpcAgentInstance) {
        return AgentInstance.newBuilder()
                .withId(grpcAgentInstance.getId())
                .withInstanceGroupId(grpcAgentInstance.getInstanceGroupId())
                .withHostname(grpcAgentInstance.getHostname())
                .withIpAddress(grpcAgentInstance.getIpAddress())
                .withDeploymentStatus(toCoreInstanceLifecycleStatus(grpcAgentInstance.getLifecycleStatus()))
                .withAttributes(grpcAgentInstance.getAttributesMap())
                .build();
    }

    private static ResourceDimension toCoreResourceDimension(com.netflix.titus.grpc.protogen.ResourceDimension grpcResourceDimension) {
        return ResourceDimension.newBuilder()
                .withCpus(grpcResourceDimension.getCpu())
                .withGpu(grpcResourceDimension.getGpu())
                .withMemoryMB(grpcResourceDimension.getMemoryMB())
                .withDiskMB(grpcResourceDimension.getDiskMB())
                .withNetworkMbs(grpcResourceDimension.getNetworkMbps())
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

    public static InstanceGroupLifecycleStatus toCoreLifecycleStatus(com.netflix.titus.grpc.protogen.InstanceGroupLifecycleStatus grpcLifecycleStatus) {
        return InstanceGroupLifecycleStatus.newBuilder()
                .withState(toCoreLifecycleState(grpcLifecycleStatus.getState()))
                .withDetail(grpcLifecycleStatus.getDetail())
                .withTimestamp(grpcLifecycleStatus.getTimestamp())
                .build();
    }

    public static InstanceLifecycleState toCoreInstanceLifecycleState(com.netflix.titus.grpc.protogen.InstanceLifecycleState grpcLifecycleState) {
        switch (grpcLifecycleState) {
            case StartInitiated:
                return InstanceLifecycleState.Launching;
            case Started:
                return InstanceLifecycleState.Started;
            case KillInitiated:
                return InstanceLifecycleState.KillInitiated;
            case Stopped:
                return InstanceLifecycleState.Stopped;
            case InstanceStateUnknown:
            default:
                return InstanceLifecycleState.Unknown;
        }
    }

    public static InstanceLifecycleStatus toCoreInstanceLifecycleStatus(com.netflix.titus.grpc.protogen.InstanceLifecycleStatus grpcLifecycleStatus) {
        return InstanceLifecycleStatus.newBuilder()
                .withState(toCoreInstanceLifecycleState(grpcLifecycleStatus.getState()))
                .withLaunchTimestamp(grpcLifecycleStatus.getLaunchTimestamp())
                .build();
    }

    public static com.netflix.titus.grpc.protogen.InstanceGroupLifecycleState toGrpcLifecycleState(InstanceGroupLifecycleState state) {
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
        throw new IllegalArgumentException("Unrecognized InstanceGroupLifecycleState value: " + state);
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
            case Unknown:
            default:
                return com.netflix.titus.grpc.protogen.InstanceLifecycleState.InstanceStateUnknown;
        }
    }

    public static com.netflix.titus.grpc.protogen.InstanceLifecycleStatus toGrpcDeploymentStatus(InstanceLifecycleStatus instanceLifecycleStatus) {
        return com.netflix.titus.grpc.protogen.InstanceLifecycleStatus.newBuilder()
                .setState(toGrpcDeploymentState(instanceLifecycleStatus.getState()))
                .setLaunchTimestamp(instanceLifecycleStatus.getLaunchTimestamp())
                .build();
    }

    public static HealthState toGrpcHealthState(AgentStatus.AgentStatusCode agentStatusCode) {
        switch (agentStatusCode) {
            case Healthy:
                return HealthState.Healthy;
            case Unhealthy:
                return HealthState.Unhealthy;
            default:
                return HealthState.Unknown;
        }
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
        throw new IllegalArgumentException("Unrecognized Tier value: " + tier);
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
