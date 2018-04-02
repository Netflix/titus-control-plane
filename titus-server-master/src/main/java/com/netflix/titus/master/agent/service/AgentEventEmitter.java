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

package com.netflix.titus.master.agent.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.netflix.titus.master.agent.service.cache.AgentCache;
import com.netflix.titus.master.agent.service.cache.CacheUpdateEvent;
import com.netflix.titus.master.agent.service.cache.CacheUpdateType;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.event.AgentEvent;
import com.netflix.titus.api.agent.model.event.AgentInstanceGroupUpdateEvent;
import com.netflix.titus.api.agent.model.event.AgentInstanceRemovedEvent;
import com.netflix.titus.api.agent.model.event.AgentInstanceUpdateEvent;
import com.netflix.titus.api.agent.model.event.AgentInstanceGroupRemovedEvent;
import com.netflix.titus.api.agent.model.event.AgentSnapshotEndEvent;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.agent.service.cache.AgentCache;
import com.netflix.titus.master.agent.service.cache.CacheUpdateEvent;
import com.netflix.titus.master.agent.service.cache.CacheUpdateType;

/**
 * As we get instance group level events from {@link AgentCache}, irrespective of what changed (instance group or instance),
 * we need to generate fine grain events by comparing the previous and the new state. This class encapsulates logic to
 * do that.
 */
final class AgentEventEmitter {

    private final AgentCache agentCache;
    private final Map<String, AgentInstanceGroup> instanceGroupsById;
    private final Map<String, Map<String, AgentInstance>> instanceGroupInstancesById;

    AgentEventEmitter(AgentCache agentCache) {
        this.agentCache = agentCache;
        this.instanceGroupsById = agentCache.getInstanceGroups().stream().collect(Collectors.toMap(AgentInstanceGroup::getId, Function.identity()));
        this.instanceGroupInstancesById = instanceGroupsById.keySet().stream().map(instanceGroupId -> {
            Map<String, AgentInstance> instances;
            try {
                instances = agentCache.getAgentInstances(instanceGroupId).stream().collect(Collectors.toMap(AgentInstance::getId, Function.identity()));
            } catch (Exception e) { // it may happen only if the instance group has just been removed
                instances = Collections.emptyMap();
            }

            return Pair.of(instanceGroupId, instances);
        }).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
    }

    Pair<List<AgentEvent>, AgentEventEmitter> apply(CacheUpdateEvent event) {
        CacheUpdateType type = event.getType();
        if (type != CacheUpdateType.InstanceGroup && type != CacheUpdateType.Instance) {
            return Pair.of(Collections.emptyList(), this);
        }

        AgentEventEmitter newEventEmitter = new AgentEventEmitter(agentCache);
        if (type == CacheUpdateType.InstanceGroup) {
            return Pair.of(compareServerGroup(event.getResourceId(), newEventEmitter), newEventEmitter);
        }
        return Pair.of(compareInstance(event.getResourceId(), newEventEmitter), newEventEmitter);
    }

    List<AgentEvent> compareWithLatestData() {
        AgentEventEmitter newEventEmitter = new AgentEventEmitter(agentCache);

        Set<String> allServerGroups = CollectionsExt.merge(instanceGroupsById.keySet(), newEventEmitter.instanceGroupsById.keySet());
        return allServerGroups.stream()
                .flatMap(instanceGroupId -> compareServerGroup(instanceGroupId, newEventEmitter).stream())
                .collect(Collectors.toList());
    }

    List<AgentEvent> emitSnapshot() {
        List<AgentEvent> events = new ArrayList<>();
        instanceGroupsById.values().forEach(sg -> {
            events.add(new AgentInstanceGroupUpdateEvent(sg));
            instanceGroupInstancesById.get(sg.getId()).values().forEach(instance -> events.add(new AgentInstanceUpdateEvent(instance)));
        });
        events.add(AgentSnapshotEndEvent.snapshotEnd());
        return events;
    }

    private List<AgentEvent> compareServerGroup(String instanceGroupId, AgentEventEmitter newEmitter) {
        AgentInstanceGroup oldServerGroup = instanceGroupsById.get(instanceGroupId);
        AgentInstanceGroup newServerGroup = newEmitter.instanceGroupsById.get(instanceGroupId);

        if (oldServerGroup == null && newServerGroup == null) {
            return Collections.emptyList();
        }

        List<AgentEvent> events = new ArrayList<>();

        if (oldServerGroup == null) {
            events.add(new AgentInstanceGroupUpdateEvent(newServerGroup));
            newEmitter.instanceGroupInstancesById.get(instanceGroupId).values().forEach(instance -> events.add(new AgentInstanceUpdateEvent(instance)));
        } else if (newServerGroup == null) {
            events.add(new AgentInstanceGroupRemovedEvent(instanceGroupId));
            instanceGroupInstancesById.get(instanceGroupId).keySet().forEach(instanceId -> events.add(new AgentInstanceRemovedEvent(instanceId)));
        } else {
            if (!oldServerGroup.equals(newServerGroup)) {
                events.add(new AgentInstanceGroupUpdateEvent(newServerGroup));
            }

            Map<String, AgentInstance> oldInstanceMap = instanceGroupInstancesById.get(instanceGroupId);
            Map<String, AgentInstance> newInstanceMap = newEmitter.instanceGroupInstancesById.get(instanceGroupId);

            oldInstanceMap.forEach((instanceId, oldInstance) -> {
                AgentInstance newInstance = newInstanceMap.get(instanceId);
                if (newInstance == null) {
                    events.add(new AgentInstanceRemovedEvent(instanceId));
                } else if (!newInstance.equals(oldInstance)) {
                    events.add(new AgentInstanceUpdateEvent(newInstance));
                }
            });
            newInstanceMap.values().forEach(instance -> {
                if (!oldInstanceMap.containsKey(instance.getId())) {
                    events.add(new AgentInstanceUpdateEvent(instance));
                }
            });
        }

        return events;
    }

    private List<AgentEvent> compareInstance(String instanceId, AgentEventEmitter newEventEmitter) {
        AgentInstance oldInstance = findInstance(instanceId);
        AgentInstance newInstance = newEventEmitter.findInstance(instanceId);

        if (newInstance == null) {
            return oldInstance == null
                    ? Collections.emptyList()
                    : Collections.singletonList(new AgentInstanceRemovedEvent(instanceId));
        }
        if (oldInstance != null && oldInstance.equals(newInstance)) {
            return Collections.emptyList();
        }
        return Collections.singletonList(new AgentInstanceUpdateEvent(newInstance));
    }

    private AgentInstance findInstance(String instanceId) {
        for (Map<String, AgentInstance> instanceMap : instanceGroupInstancesById.values()) {
            AgentInstance instance = instanceMap.get(instanceId);
            if (instance != null) {
                return instance;
            }
        }
        return null;
    }
}
