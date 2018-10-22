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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.event.AgentEvent;
import com.netflix.titus.api.agent.model.event.AgentInstanceGroupRemovedEvent;
import com.netflix.titus.api.agent.model.event.AgentInstanceGroupUpdateEvent;
import com.netflix.titus.api.agent.model.event.AgentInstanceRemovedEvent;
import com.netflix.titus.api.agent.model.event.AgentInstanceUpdateEvent;
import com.netflix.titus.api.agent.model.event.AgentSnapshotEndEvent;
import com.netflix.titus.common.data.generator.MutableDataGenerator;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.common.util.tuple.Either;
import com.netflix.titus.common.util.tuple.Pair;
import rx.Observable;
import rx.subjects.PublishSubject;

class StubbedAgentData {

    private final ConcurrentMap<String, InstanceGroupDeployment> instanceGroups = new ConcurrentHashMap<>();

    private final PublishSubject<AgentEvent> observeAgentsSubject = PublishSubject.create();

    AgentInstance getFirstInstance() {
        return instanceGroups.values().stream()
                .filter(g -> !g.isEmpty()).map(g -> g.getInstances().get(0))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No agent instances available"));
    }

    List<AgentInstanceGroup> getInstanceGroups() {
        return instanceGroups.values().stream().map(InstanceGroupDeployment::getInstanceGroup).collect(Collectors.toList());
    }

    AgentInstanceGroup getInstanceGroup(String instanceGroupId) {
        AgentInstanceGroup instanceGroup = instanceGroups.get(instanceGroupId).getInstanceGroup();
        Preconditions.checkState(instanceGroup != null, "Requested unknown agent instance group");
        return instanceGroup;
    }

    List<Pair<AgentInstanceGroup, List<AgentInstance>>> getInstances() {
        return instanceGroups.values().stream().map(d -> Pair.of(d.getInstanceGroup(), d.getInstances())).collect(Collectors.toList());
    }

    List<AgentInstance> getInstances(String instanceGroupId) {
        return getDeploymentByGroupId(instanceGroupId).getInstances();
    }

    AgentInstance getInstance(String instanceId) {
        return instanceGroups.values().stream()
                .flatMap(d -> d.getInstances().stream())
                .filter(i -> i.getId().equals(instanceId))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Unknown agent instance " + instanceId));
    }

    Observable<AgentEvent> observeAgents(boolean snapshot) {
        return snapshot ? ObservableExt.fromCollection(this::getEventSnapshot).concatWith(observeAgentsSubject) : observeAgentsSubject;
    }

    private Collection<AgentEvent> getEventSnapshot() {
        List<AgentEvent> events = new ArrayList<>();
        instanceGroups.values().forEach(deployment -> {
            events.add(new AgentInstanceGroupUpdateEvent(deployment.getInstanceGroup()));
            deployment.getInstances().forEach(instance -> events.add(new AgentInstanceUpdateEvent(instance)));
        });
        events.add(AgentSnapshotEndEvent.snapshotEnd());
        return events;
    }

    public void addInstanceGroup(AgentInstanceGroup instanceGroup) {
        Preconditions.checkState(
                instanceGroups.get(instanceGroup.getId()) == null,
                "Instance group with id %s already exists", instanceGroup.getId()
        );
        instanceGroups.put(instanceGroup.getId(), new InstanceGroupDeployment(instanceGroup));
    }

    void removeInstanceGroup(String instanceGroupId) {
        InstanceGroupDeployment deployment = getDeploymentByGroupId(instanceGroupId);
        instanceGroups.remove(instanceGroupId);
        deployment.destroy();
    }

    void terminateInstance(String instanceId, boolean shrink) {
        getDeploymentByInstanceId(instanceId).terminateInstance(instanceId, shrink);
    }

    AgentInstanceGroup changeInstanceGroup(String instanceGroupId, Function<AgentInstanceGroup, AgentInstanceGroup> transformer) {
        return getDeploymentByGroupId(instanceGroupId).changeInstanceGroup(transformer);
    }

    AgentInstance changeInstance(String instanceId, Function<AgentInstance, AgentInstance> transformer) {
        return getDeploymentByInstanceId(instanceId).changeInstance(instanceId, transformer);
    }

    Observable<List<Either<Boolean, Throwable>>> terminateInstances(String instanceGroupId, List<String> instanceIds, boolean shrink) {
        return Observable.defer(() -> Observable.just(getDeploymentByGroupId(instanceGroupId).terminateInstances(instanceIds, shrink)));
    }

    private InstanceGroupDeployment getDeploymentByGroupId(String instanceGroupId) {
        return Preconditions.checkNotNull(instanceGroups.get(instanceGroupId), "Unknown agent instance group " + instanceGroupId);
    }

    private InstanceGroupDeployment getDeploymentByInstanceId(String instanceId) {
        return instanceGroups.values().stream()
                .filter(d -> d.hasInstance(instanceId))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Unknown agent instance group " + instanceId));
    }

    private class InstanceGroupDeployment {

        private AgentInstanceGroup instanceGroup;
        private final Map<String, AgentInstance> instancesById = new HashMap<>();

        private final MutableDataGenerator<AgentInstance> instanceDataGenerator;

        InstanceGroupDeployment(AgentInstanceGroup instanceGroup) {
            this.instanceDataGenerator = new MutableDataGenerator<>(AgentGenerator.agentInstances(instanceGroup));
            this.instanceGroup = instanceGroup;
            instanceDataGenerator.getValues(instanceGroup.getDesired()).forEach(instance -> instancesById.put(instance.getId(), instance));

            observeAgentsSubject.onNext(new AgentInstanceGroupUpdateEvent(instanceGroup));
        }

        boolean isEmpty() {
            return instancesById.isEmpty();
        }

        public List<AgentInstance> getInstances() {
            return new ArrayList<>(instancesById.values());
        }

        boolean hasInstance(String instanceId) {
            return instancesById.containsKey(instanceId);
        }

        AgentInstanceGroup getInstanceGroup() {
            return instanceGroup;
        }

        void destroy() {
            instancesById.forEach((id, instance) -> observeAgentsSubject.onNext(new AgentInstanceRemovedEvent(id)));
            observeAgentsSubject.onNext(new AgentInstanceGroupRemovedEvent(instanceGroup.getId()));
        }

        void terminateInstance(String instanceId, boolean shrink) {
            Preconditions.checkNotNull(instancesById.remove(instanceId));
            observeAgentsSubject.onNext(new AgentInstanceRemovedEvent(instanceId));

            if (shrink) {
                this.instanceGroup = instanceGroup.toBuilder().withDesired(instanceGroup.getDesired() - 1).build();

                observeAgentsSubject.onNext(new AgentInstanceGroupUpdateEvent(instanceGroup));
            } else {
                AgentInstance newInstance = instanceDataGenerator.getValue();
                instancesById.put(newInstance.getId(), newInstance);

                observeAgentsSubject.onNext(new AgentInstanceUpdateEvent(newInstance));
            }
        }

        AgentInstanceGroup changeInstanceGroup(Function<AgentInstanceGroup, AgentInstanceGroup> transformer) {
            this.instanceGroup = transformer.apply(instanceGroup);
            observeAgentsSubject.onNext(new AgentInstanceGroupUpdateEvent(instanceGroup));
            return instanceGroup;
        }

        AgentInstance changeInstance(String instanceId, Function<AgentInstance, AgentInstance> transformer) {
            AgentInstance instance = Preconditions.checkNotNull(instancesById.get(instanceId));

            AgentInstance updated = transformer.apply(instance);
            instancesById.put(instanceId, updated);

            observeAgentsSubject.onNext(new AgentInstanceUpdateEvent(updated));
            return updated;
        }

        List<Either<Boolean, Throwable>> terminateInstances(List<String> instanceIds, boolean shrink) {
            return instanceIds.stream().map(instanceId -> {
                try {
                    terminateInstance(instanceId, shrink);
                    return Either.<Boolean, Throwable>ofValue(true);
                } catch (Exception e) {
                    return Either.<Boolean, Throwable>ofError(e);
                }
            }).collect(Collectors.toList());
        }
    }
}
