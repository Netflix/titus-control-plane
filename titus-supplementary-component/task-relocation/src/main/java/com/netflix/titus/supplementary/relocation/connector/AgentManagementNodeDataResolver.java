/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.supplementary.relocation.connector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.agent.service.ReadOnlyAgentOperations;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.runtime.connector.agent.AgentDataReplicator;
import com.netflix.titus.supplementary.relocation.RelocationAttributes;

public class AgentManagementNodeDataResolver implements NodeDataResolver {

    private final ReadOnlyAgentOperations agentOperations;
    private final AgentDataReplicator agentDataReplicator;
    private final Predicate<AgentInstance> fenzoNodeFilter;

    public AgentManagementNodeDataResolver(ReadOnlyAgentOperations agentOperations,
                                           AgentDataReplicator agentDataReplicator,
                                           Predicate<AgentInstance> fenzoNodeFilter) {
        this.agentOperations = agentOperations;
        this.agentDataReplicator = agentDataReplicator;
        this.fenzoNodeFilter = fenzoNodeFilter;
    }

    @Override
    public Map<String, Node> resolve() {
        List<Pair<AgentInstanceGroup, List<AgentInstance>>> all = agentOperations.findAgentInstances(pair ->
                fenzoNodeFilter.test(pair.getRight())
        );
        Map<String, Node> result = new HashMap<>();
        all.forEach(pair -> {
            AgentInstanceGroup serverGroup = pair.getLeft();
            List<AgentInstance> instances = pair.getRight();
            instances.forEach(instance -> result.put(instance.getId(), toNode(serverGroup, instance)));
        });
        return result;
    }

    @Override
    public long getStalenessMs() {
        return agentDataReplicator.getStalenessMs();
    }

    private Node toNode(AgentInstanceGroup serverGroup, AgentInstance instance) {
        boolean relocationRequired = instance.getAttributes()
                .getOrDefault(RelocationAttributes.RELOCATION_REQUIRED, "false")
                .equalsIgnoreCase("true");

        boolean relocationRequiredImmediately = instance.getAttributes()
                .getOrDefault(RelocationAttributes.RELOCATION_REQUIRED_IMMEDIATELY, "false")
                .equalsIgnoreCase("true");

        boolean relocationNodeAllowed = instance.getAttributes()
                .getOrDefault(RelocationAttributes.RELOCATION_NOT_ALLOWED, "false")
                .equalsIgnoreCase("true");

        boolean serverGroupRelocationRequired = serverGroup.getLifecycleStatus().getState() == InstanceGroupLifecycleState.Removable;

        return Node.newBuilder()
                .withId(instance.getId())
                .withServerGroupId(serverGroup.getId())
                .withRelocationRequired(relocationRequired)
                .withRelocationRequiredImmediately(relocationRequiredImmediately)
                .withRelocationNotAllowed(relocationNodeAllowed)
                .withServerGroupRelocationRequired(serverGroupRelocationRequired)
                .build();
    }
}
