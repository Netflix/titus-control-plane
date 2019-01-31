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

package com.netflix.titus.api.agent.model;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.netflix.titus.api.agent.service.ReadOnlyAgentOperations;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.time.Clock;

/**
 * Collection of functions for agent entity transformations.
 */
public final class AgentFunctions {

    private AgentFunctions() {
    }

    public static Map<String, AgentInstance> buildInstanceByIdMap(ReadOnlyAgentOperations agentManagementService) {
        Map<String, AgentInstance> instancesById = new HashMap<>();
        agentManagementService.getInstanceGroups().forEach(instanceGroup -> {
            try {
                agentManagementService.getAgentInstances(instanceGroup.getId()).forEach(instance -> {
                    instancesById.put(instance.getId(), instance);
                });
            } catch (Exception e) {
                // Ignore
            }
        });
        return instancesById;
    }

    public static Set<String> instanceGroupIds(Collection<AgentInstanceGroup> instanceGroups) {
        return instanceGroups.stream().map(AgentInstanceGroup::getId).collect(Collectors.toSet());
    }

    public static Set<String> instanceIds(Collection<AgentInstance> instances) {
        return instances.stream().map(AgentInstance::getId).collect(Collectors.toSet());
    }

    public static Function<AgentInstanceGroup, AgentInstanceGroup> withId(String newId) {
        return ig -> ig.toBuilder().withId(newId).build();
    }

    public static Function<AgentInstanceGroup, AgentInstanceGroup> inState(InstanceGroupLifecycleState state, String detail, Clock clock) {
        return ig -> ig.toBuilder().withLifecycleStatus(
                InstanceGroupLifecycleStatus.newBuilder()
                        .withState(state)
                        .withTimestamp(clock.wallTime())
                        .withDetail(detail)
                        .build()
        ).build();
    }

    public static AgentInstance appendInstanceAttribute(AgentInstance instance, String attributeName, Object attributeValue) {
        return instance.toBuilder()
                .withAttributes(CollectionsExt.copyAndAdd(instance.getAttributes(), attributeName, "" + attributeValue))
                .build();
    }
}
