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

package io.netflix.titus.master.agent.service.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.netflix.titus.api.agent.model.AgentInstance;
import io.netflix.titus.api.agent.model.AgentInstanceGroup;
import io.netflix.titus.common.util.tuple.Pair;

class AgentDataSnapshot {

    private final Set<String> instanceGroupIds;
    private final List<AgentInstanceGroup> instanceGroups;
    private final Map<String, Pair<AgentInstanceGroup, Set<AgentInstance>>> instanceGroupsById;
    private final Map<String, AgentInstance> agentInstancesById;

    AgentDataSnapshot() {
        this(Collections.emptyMap());
    }

    private AgentDataSnapshot(Map<String, Pair<AgentInstanceGroup, Set<AgentInstance>>> instanceGroupsById) {
        this.instanceGroupsById = Collections.unmodifiableMap(instanceGroupsById);
        this.instanceGroupIds = Collections.unmodifiableSet(new HashSet<>(instanceGroupsById.keySet()));
        this.instanceGroups = Collections.unmodifiableList(new ArrayList<>(instanceGroupsById.values().stream().map(Pair::getLeft).collect(Collectors.toList())));
        this.agentInstancesById = Collections.unmodifiableMap(instanceGroupsById.values().stream()
                .flatMap(p -> p.getRight().stream())
                .collect(Collectors.toMap(AgentInstance::getId, Function.identity()))
        );
    }

    List<AgentInstanceGroup> getInstanceGroups() {
        return instanceGroups;
    }

    AgentInstanceGroup getInstanceGroup(String instanceGroupId) {
        Pair<AgentInstanceGroup, Set<AgentInstance>> pair = instanceGroupsById.get(instanceGroupId);
        return pair == null ? null : pair.getLeft();
    }

    Set<String> getInstanceGroupIds() {
        return instanceGroupIds;
    }

    AgentInstance getInstance(String instanceId) {
        return agentInstancesById.get(instanceId);
    }

    Set<AgentInstance> getInstances(String instanceGroupId) {
        Pair<AgentInstanceGroup, Set<AgentInstance>> pair = instanceGroupsById.get(instanceGroupId);
        return pair == null ? null : pair.getRight();
    }

    AgentDataSnapshot updateInstanceGroup(AgentInstanceGroup agentInstanceGroup, Set<AgentInstance> agentInstances) {
        Map<String, Pair<AgentInstanceGroup, Set<AgentInstance>>> newInstanceGroupsById = new HashMap<>(instanceGroupsById);
        newInstanceGroupsById.put(agentInstanceGroup.getId(), Pair.of(agentInstanceGroup, agentInstances));
        return new AgentDataSnapshot(newInstanceGroupsById);
    }

    AgentDataSnapshot updateAgentInstance(AgentInstance agentInstance) {
        String instanceGroupId = agentInstance.getInstanceGroupId();
        Pair<AgentInstanceGroup, Set<AgentInstance>> previous = instanceGroupsById.get(instanceGroupId);
        if (previous == null) {
            return this;
        }
        Set<AgentInstance> instanceSet = new TreeSet<>(AgentInstance.idComparator());
        instanceSet.add(agentInstance);

        Map<String, Pair<AgentInstanceGroup, Set<AgentInstance>>> newInstanceGroupsById = new HashMap<>(instanceGroupsById);
        newInstanceGroupsById.put(instanceGroupId, Pair.of(previous.getLeft(), instanceSet));

        return new AgentDataSnapshot(newInstanceGroupsById);
    }

    AgentDataSnapshot removeInstanceGroup(String instanceGroupId) {
        if (!instanceGroupsById.containsKey(instanceGroupId)) {
            return this;
        }

        Map<String, Pair<AgentInstanceGroup, Set<AgentInstance>>> newInstanceGroupByIds = new HashMap<>(instanceGroupsById);
        newInstanceGroupByIds.remove(instanceGroupId);

        return new AgentDataSnapshot(newInstanceGroupByIds);
    }

    AgentDataSnapshot removeInstances(String instanceGroupId, Set<String> agentInstanceIds) {
        Pair<AgentInstanceGroup, Set<AgentInstance>> existing = instanceGroupsById.get(instanceGroupId);
        if (existing == null) {
            return this;
        }

        Map<String, Pair<AgentInstanceGroup, Set<AgentInstance>>> newInstanceGroupsById = new HashMap<>(instanceGroupsById);
        TreeSet<AgentInstance> newInstanceSet = new TreeSet<>(AgentInstance.idComparator());
        existing.getRight().forEach(agentInstance -> {
            if (!agentInstanceIds.contains(agentInstance.getId())) {
                newInstanceSet.add(agentInstance);
            }
        });
        newInstanceGroupsById.put(instanceGroupId, Pair.of(existing.getLeft(), newInstanceSet));

        return new AgentDataSnapshot(newInstanceGroupsById);
    }

    static AgentDataSnapshot initWithStaleDataSnapshot(List<AgentInstanceGroup> persistedInstanceGroups, List<AgentInstance> persistedInstances) {
        Map<String, Set<AgentInstance>> instancesByInstanceGroup = persistedInstances.stream()
                .collect(Collectors.groupingBy(AgentInstance::getInstanceGroupId, Collectors.toSet()));

        Map<String, Pair<AgentInstanceGroup, Set<AgentInstance>>> instanceGroupsById = persistedInstanceGroups.stream()
                .map(sg -> {
                    Set<AgentInstance> instances = instancesByInstanceGroup.get(sg.getId());
                    instances = instances == null ? Collections.emptySet() : instances;
                    return Pair.of(sg, instances);
                }).collect(Collectors.toMap(p -> p.getLeft().getId(), Function.identity()));


        return new AgentDataSnapshot(instanceGroupsById);
    }
}
