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

package com.netflix.titus.testkit.embedded.kube;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.netflix.titus.testkit.embedded.kube.event.EmbeddedKubeEvent;
import io.kubernetes.client.openapi.models.V1Pod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

public class EmbeddedKubeFleet {

    private static final Logger logger = LoggerFactory.getLogger(EmbeddedKubeFleet.class);

    private final ConcurrentMap<String, EmbeddedServerGroup> serverGroups = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, EmbeddedKubeNode> nodes = new ConcurrentHashMap<>();

    private final Sinks.Many<EmbeddedKubeEvent<EmbeddedKubeNode>> nodePublisher = Sinks.many().multicast().directAllOrNothing();

    public Map<String, EmbeddedServerGroup> getServerGroups() {
        return new HashMap<>(serverGroups);
    }

    public Map<String, EmbeddedKubeNode> getNodes() {
        return new HashMap<>(nodes);
    }

    public synchronized void addServerGroup(EmbeddedServerGroup serverGroup) {
        logger.info("Adding server group {}", serverGroup);
        serverGroups.put(serverGroup.getName(), serverGroup);
        for (int i = 0; i < serverGroup.getSize(); i++) {
            addNodeToServerGroup(serverGroup.getName());
        }
    }

    public synchronized EmbeddedKubeNode addNodeToServerGroup(String name) {
        EmbeddedServerGroup serverGroup = Preconditions.checkNotNull(serverGroups.get(name), "Server group not found %s", name);
        EmbeddedKubeNode newNode = EmbeddedKubeNode.newBuilder()
                .withName(UUID.randomUUID().toString())
                .withResourcePool(serverGroup.getResourcePool())
                .withZone(findUnderProvisionedZone(serverGroup))
                .withServerGroup(serverGroup)
                .withIpAddress(EmbeddedKubeUtil.nextIpAddress())
                .withNodeResources(serverGroup.getNodeResources())
                .build();
        logger.info("Adding node {}", newNode.toStringCompact());
        nodes.put(newNode.getName(), newNode);
        nodePublisher.tryEmitNext(EmbeddedKubeEvent.added(newNode));
        return newNode;
    }

    private String findUnderProvisionedZone(EmbeddedServerGroup serverGroup) {
        String minZone = null;
        int minSize = Integer.MAX_VALUE;
        for (String zone : serverGroup.getZones()) {
            Map<String, EmbeddedKubeNode> nodes = getServerGroupNodesInZone(serverGroup, zone);
            if (nodes.size() < minSize) {
                minZone = zone;
                minSize = nodes.size();
            }

        }
        return Preconditions.checkNotNull(minZone);
    }

    private Map<String, EmbeddedKubeNode> getServerGroupNodesInZone(EmbeddedServerGroup serverGroup, String zoneId) {
        return nodes.values().stream()
                .filter(node -> node.getServerGroup().getName().equals(serverGroup.getName()))
                .filter(node -> node.getZone().equals(zoneId))
                .collect(Collectors.toMap(EmbeddedKubeNode::getName, n -> n));
    }

    public synchronized EmbeddedKubeNode addNode(EmbeddedKubeNode node) {
        Preconditions.checkArgument(serverGroups.containsKey(node.getServerGroup().getName()), "Server group not found %s", node.getServerGroup().getName());
        EmbeddedKubeNode previous = nodes.put(node.getName(), node);
        if (previous != null) {
            logger.info("Updating node {}", node.toStringCompact());
            nodePublisher.tryEmitNext(EmbeddedKubeEvent.updated(node, previous));
        } else {
            logger.info("Adding node {}", node.toStringCompact());
            nodePublisher.tryEmitNext(EmbeddedKubeEvent.added(node));
        }
        return node;
    }

    public synchronized boolean deleteNode(String name) {
        EmbeddedKubeNode deleted = nodes.remove(name);
        if (deleted == null) {
            return false;
        }
        logger.info("Deleted node {}", name);
        nodePublisher.tryEmitNext(EmbeddedKubeEvent.deleted(deleted));
        return true;
    }

    public synchronized void assignPodToNode(V1Pod pod, EmbeddedKubeNode node) {
        EmbeddedKubeNode current = Preconditions.checkNotNull(nodes.get(node.getName()), "Node %s not found", node.getName());
        EmbeddedKubeNode updated = current.toBuilder()
                .withPod(pod)
                .build();
        nodes.put(node.getName(), updated);
        logger.info("Assign pod to a node: podI={}, nodeId={}, serverGroup={}", pod.getMetadata().getName(), node.getName(), node.getServerGroup().getName());
    }

    public void removePodFromNode(V1Pod pod) {
        String nodeName = pod.getSpec().getNodeName();
        EmbeddedKubeNode node = Preconditions.checkNotNull(
                nodes.get(nodeName),
                "Node not found: nodeName=%s, assignedToPod=%s", nodeName, pod.getMetadata().getName()
        );
        Map<String, V1Pod> assignedPods = new HashMap<>(node.getAssignedPods());
        assignedPods.remove(pod.getMetadata().getName());
        EmbeddedKubeNode updated = node.toBuilder().withAssignedPods(assignedPods).build();
        nodes.put(node.getName(), updated);
        logger.info("Removed pod from a node: podI={}, nodeId={}, serverGroup={}", pod.getMetadata().getName(), node.getName(), node.getServerGroup().getName());
    }

    public Flux<EmbeddedKubeEvent<EmbeddedKubeNode>> observeNodes() {
        return nodePublisher.asFlux();
    }
}
