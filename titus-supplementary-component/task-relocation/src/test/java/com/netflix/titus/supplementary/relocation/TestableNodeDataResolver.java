/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.supplementary.relocation;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.netflix.titus.supplementary.relocation.connector.Node;
import com.netflix.titus.supplementary.relocation.connector.NodeDataResolver;

public class TestableNodeDataResolver implements NodeDataResolver {

    private final ConcurrentMap<String, ConcurrentMap<String, Node>> nodesByServerGroups = new ConcurrentHashMap<>();

    @Override
    public Map<String, Node> resolve() {
        Map<String, Node> all = new HashMap<>();
        nodesByServerGroups.forEach((serverGroupId, nodes) -> all.putAll(nodes));
        return all;
    }

    @Override
    public long getStalenessMs() {
        return 0;
    }

    public Map<String, Node> getNodes(String serverGroupId) {
        return nodesByServerGroups.get(serverGroupId);
    }

    public void addNode(Node node) {
        nodesByServerGroups.computeIfAbsent(node.getServerGroupId(), id -> new ConcurrentHashMap<>()).put(node.getId(), node);
    }

    public Node getNode(String agentId) {
        for (ConcurrentMap<String, Node> nodes : nodesByServerGroups.values()) {
            if (nodes.containsKey(agentId)) {
                return nodes.get(agentId);
            }
        }
        return null;
    }
}
