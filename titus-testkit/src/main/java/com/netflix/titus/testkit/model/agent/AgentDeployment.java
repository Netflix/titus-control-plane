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

package com.netflix.titus.testkit.model.agent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceLifecycleStatus;
import com.netflix.titus.api.agent.model.event.AgentEvent;
import com.netflix.titus.api.agent.model.event.AgentInstanceGroupRemovedEvent;
import com.netflix.titus.api.agent.model.event.AgentInstanceGroupUpdateEvent;
import com.netflix.titus.api.agent.model.event.AgentInstanceRemovedEvent;
import com.netflix.titus.api.agent.model.event.AgentInstanceUpdateEvent;
import com.netflix.titus.api.agent.model.monitor.AgentStatus;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.agent.service.AgentStatusMonitor;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.aws.AwsInstanceType;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.grpc.protogen.AgentChangeEvent;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcAgentModelConverters;
import rx.Observable;
import rx.subjects.PublishSubject;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class AgentDeployment {

    private static final AgentChangeEvent GRPC_SNAPSHOT_MARKER = AgentChangeEvent.newBuilder().setSnapshotEnd(AgentChangeEvent.SnapshotEnd.getDefaultInstance()).build();

    private final Map<String, AgentInstanceGroup> instanceGroups;
    private final Map<String, Map<String, AgentInstance>> instancesByGroup;

    private final AgentStatusMonitor agentStatusMonitor = new StubbedAgentStatusMonitor();

    private final PublishSubject<AgentEvent> observeAgentsSubject = PublishSubject.create();

    private AgentDeployment(DeploymentBuilder deploymentBuilder) {
        this.instanceGroups = deploymentBuilder.instanceGroups;
        this.instancesByGroup = deploymentBuilder.instancesByGroup;
    }

    public AgentInstance getFirstInstance() {
        return instancesByGroup.values().stream().filter(v -> !v.isEmpty())
                .findFirst()
                .map(p -> p.values().iterator().next())
                .orElseThrow(() -> new IllegalStateException("No agent instances available"));
    }

    public List<AgentInstanceGroup> getInstanceGroups() {
        return new ArrayList<>(instanceGroups.values());
    }

    public AgentInstanceGroup getInstanceGroup(String instanceGroupId) {
        AgentInstanceGroup instanceGroup = instanceGroups.get(instanceGroupId);
        Preconditions.checkState(instanceGroup != null, "Requested unknown agent instance group");
        return instanceGroup;
    }

    public List<AgentInstance> getInstances(String instanceGroupId) {
        Map<String, AgentInstance> instanceMap = instancesByGroup.get(instanceGroupId);
        Preconditions.checkState(instanceMap != null, "Unknown agent instance group " + instanceGroupId);
        return new ArrayList<>(instanceMap.values());
    }

    public AgentInstance getInstance(String instanceId) {
        for (Map<String, AgentInstance> instanceMap : instancesByGroup.values()) {
            for (AgentInstance instance : instanceMap.values()) {
                if (instanceId.equals(instance.getId())) {
                    return instance;
                }
            }
        }
        throw new IllegalStateException("Unknown agent instance " + instanceId);
    }

    public List<AgentInstanceGroup> getInstanceGroupsOfType(String instanceType) {
        return instanceGroups.values().stream().filter(g -> g.getInstanceType().equals(instanceType)).collect(Collectors.toList());
    }

    public Observable<AgentEvent> observeAgents(boolean snapshot) {
        return snapshot ? ObservableExt.fromCollection(this::getEventSnapshot).concatWith(observeAgentsSubject) : observeAgentsSubject;
    }

    public Observable<AgentChangeEvent> grpcObserveAgents(boolean snapshot) {
        Observable<AgentChangeEvent> result = observeAgentsSubject
                .map(event -> GrpcAgentModelConverters.toGrpcEvent(event, agentStatusMonitor))
                .filter(Optional::isPresent)
                .map(Optional::get);
        if (!snapshot) {
            return result;
        }

        return ObservableExt.fromCollection(this::getEventSnapshot)
                .map(event -> GrpcAgentModelConverters.toGrpcEvent(event, agentStatusMonitor))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .concatWith(Observable.just(GRPC_SNAPSHOT_MARKER))
                .concatWith(result);
    }

    private Collection<AgentEvent> getEventSnapshot() {
        List<AgentEvent> events = new ArrayList<>();
        instanceGroups.values().forEach(pair -> events.add(new AgentInstanceGroupUpdateEvent(pair)));
        instancesByGroup.values().forEach(instanceMap -> instanceMap.values().forEach(instance -> events.add(new AgentInstanceUpdateEvent(instance))));
        return events;
    }

    public void changeTier(String instanceGroupId, Tier tier) {
        AgentInstanceGroup instanceGroup = getInstanceGroup(instanceGroupId);
        if (tier != instanceGroup.getTier()) {
            AgentInstanceGroup updated = instanceGroup.toBuilder().withTier(tier).build();
            instanceGroups.put(instanceGroupId, updated);
            observeAgentsSubject.onNext(new AgentInstanceGroupUpdateEvent(updated));
        }
    }

    public void removeInstanceGroup(String instanceGroupId) {
        AgentInstanceGroup instanceGroup = getInstanceGroup(instanceGroupId);

        List<AgentInstance> instances = new ArrayList<>(instancesByGroup.get(instanceGroupId).values());
        instances.forEach(instanceId -> removeInstance(instanceId.getId()));

        instancesByGroup.remove(instanceGroupId);
        instanceGroups.remove(instanceGroupId);

        observeAgentsSubject.onNext(new AgentInstanceGroupRemovedEvent(instanceGroupId));
    }

    public void removeInstance(String instanceId) {
        AgentInstance instance = getInstance(instanceId);
        Map<String, AgentInstance> instances = instancesByGroup.get(instance.getInstanceGroupId());
        instances.remove(instanceId);

        AgentInstanceGroup instanceGroup = getInstanceGroup(instance.getInstanceGroupId());
        AgentInstanceGroup updatedInstanceGroup = instanceGroup.toBuilder().withCurrent(instanceGroup.getCurrent() - 1).build();
        instanceGroups.put(instance.getInstanceGroupId(), updatedInstanceGroup);

        observeAgentsSubject.onNext(new AgentInstanceRemovedEvent(instanceId));
    }

    public AgentInstance changeInstanceLifecycleStatus(String instanceId, InstanceLifecycleStatus status) {
        AgentInstance updated = getInstance(instanceId).toBuilder().withDeploymentStatus(status).build();
        updateInstanceInternal(updated);
        observeAgentsSubject.onNext(new AgentInstanceUpdateEvent(updated));
        return updated;
    }

    private void updateInstanceInternal(AgentInstance updated) {
        Map<String, AgentInstance> instances = instancesByGroup.get(updated.getInstanceGroupId());
        instances.put(updated.getId(), updated);
    }

    public static AgentManagementService instrumentMock(AgentDeployment deployment, AgentManagementService agentManagementService) {
        when(agentManagementService.getInstanceGroups()).thenReturn(deployment.getInstanceGroups());
        when(agentManagementService.getInstanceGroup(anyString())).thenAnswer(argument -> deployment.getInstanceGroup(argument.getArgument(0)));
        when(agentManagementService.getAgentInstances(anyString())).thenAnswer(argument -> deployment.getInstances(argument.getArgument(0)));
        when(agentManagementService.getAgentInstance(anyString())).thenAnswer(argument -> deployment.getInstance(argument.getArgument(0)));
        when(agentManagementService.events(anyBoolean())).thenAnswer(argument -> deployment.observeAgents(argument.getArgument(0)));
        return agentManagementService;
    }

    public static DeploymentBuilder newDeployment() {
        return new DeploymentBuilder();
    }

    public static class DeploymentBuilder {

        private final Map<String, AgentInstanceGroup> instanceGroups = new HashMap<>();
        private final Map<String, Map<String, AgentInstance>> instancesByGroup = new HashMap<>();

        public DeploymentBuilder withInstanceGroup(AgentInstanceGroup.Builder instanceGroupBuilder) {
            AgentInstanceGroup instanceGroup = instanceGroupBuilder.build();
            instanceGroups.put(instanceGroup.getId(), instanceGroup);
            return this;
        }

        public DeploymentBuilder withActiveInstanceGroup(Tier tier, String id, AwsInstanceType instanceType, int desired) {
            AgentInstanceGroup instanceGroup = AgentGenerator.agentServerGroups(tier, desired, instanceType).getValue().toBuilder()
                    .withId(id)
                    .build();

            instanceGroups.put(instanceGroup.getId(), instanceGroup);

            HashMap<String, AgentInstance> instanceMap = new HashMap<>();
            instancesByGroup.put(instanceGroup.getId(), instanceMap);
            AgentGenerator.agentInstances(instanceGroup).toList(desired).forEach(instance -> instanceMap.put(instance.getId(), instance));

            return this;
        }

        public AgentDeployment build() {
            return new AgentDeployment(this);
        }
    }

    private class StubbedAgentStatusMonitor implements AgentStatusMonitor {

        @Override
        public AgentStatus getStatus(String agentInstanceId) {
            AgentInstance instance = getInstance(agentInstanceId);
            return AgentStatus.healthy("test", instance, "test", 0);
        }

        @Override
        public boolean isHealthy(String agentInstanceId) {
            return getStatus(agentInstanceId).getStatusCode() == AgentStatus.AgentStatusCode.Healthy;
        }

        @Override
        public Observable<AgentStatus> monitor() {
            return observeAgents(false)
                    .filter(event -> event instanceof AgentInstanceUpdateEvent || event instanceof AgentInstanceRemovedEvent)
                    .map(event -> {
                        if (event instanceof AgentInstanceUpdateEvent) {
                            AgentInstanceUpdateEvent updateEvent = (AgentInstanceUpdateEvent) event;
                            return AgentStatus.healthy("test", updateEvent.getAgentInstance(), "test", 0);
                        }
                        AgentInstanceRemovedEvent removedEvent = (AgentInstanceRemovedEvent) event;
                        return AgentStatus.terminated("test", getInstance(removedEvent.getAgentInstanceId()), "test", 0);
                    });
        }
    }
}
