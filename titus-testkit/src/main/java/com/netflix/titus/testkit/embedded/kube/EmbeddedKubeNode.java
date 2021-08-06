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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.master.model.ResourceDimensions;
import com.netflix.titus.runtime.kubernetes.KubeConstants;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1NodeAddress;
import io.kubernetes.client.openapi.models.V1NodeStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;

import static com.netflix.titus.testkit.embedded.kube.EmbeddedKubeUtil.fromPodToResourceDimension;
import static com.netflix.titus.testkit.embedded.kube.EmbeddedKubeUtil.fromResourceDimensionsToKubeQuantityMap;

public class EmbeddedKubeNode {

    private final String name;
    private final String resourcePool;
    private final EmbeddedServerGroup serverGroup;
    private final String zone;
    private final boolean deployed;
    private final String ipAddress;
    private final ResourceDimension nodeResources;
    private final ResourceDimension nodeUnassignedResources;
    private final Map<String, V1Pod> assignedPods;
    private final V1Node v1Node;

    public EmbeddedKubeNode(String name,
                            String resourcePool,
                            EmbeddedServerGroup serverGroup,
                            String zone,
                            boolean deployed,
                            String ipAddress,
                            ResourceDimension nodeResources,
                            ResourceDimension nodeUnassignedResources,
                            Map<String, V1Pod> assignedPods) {
        this.name = name;
        this.resourcePool = resourcePool;
        this.serverGroup = serverGroup;
        this.zone = zone;
        this.deployed = deployed;
        this.ipAddress = ipAddress;
        this.nodeResources = nodeResources;
        this.nodeUnassignedResources = nodeUnassignedResources;
        this.assignedPods = assignedPods;

        Map<String, String> labels = new HashMap<>();
        if (resourcePool != null) {
            labels.put(KubeConstants.NODE_LABEL_RESOURCE_POOL, resourcePool);
        }
        this.v1Node = new V1Node()
                .metadata(new V1ObjectMeta()
                        .name(name)
                        .annotations(CollectionsExt.asMap(
                                KubeConstants.NODE_LABEL_ZONE, zone
                        ))
                        .labels(labels)
                )
                .status(new V1NodeStatus()
                        .allocatable(fromResourceDimensionsToKubeQuantityMap(nodeUnassignedResources))
                        .addresses(ipAddress == null
                                ? Collections.emptyList()
                                : Collections.singletonList(new V1NodeAddress().address(ipAddress))
                        )
                );
    }

    public String getName() {
        return name;
    }

    public String getResourcePool() {
        return resourcePool;
    }

    public EmbeddedServerGroup getServerGroup() {
        return serverGroup;
    }

    public String getZone() {
        return zone;
    }

    public boolean isDeployed() {
        return deployed;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public ResourceDimension getNodeResources() {
        return nodeResources;
    }

    public ResourceDimension getUnassignedResources() {
        return nodeUnassignedResources;
    }

    public Map<String, V1Pod> getAssignedPods() {
        return assignedPods;
    }

    public V1Node getV1Node() {
        return v1Node;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EmbeddedKubeNode that = (EmbeddedKubeNode) o;
        return deployed == that.deployed && Objects.equals(name, that.name) && Objects.equals(resourcePool, that.resourcePool) && Objects.equals(serverGroup, that.serverGroup) && Objects.equals(ipAddress, that.ipAddress) && Objects.equals(nodeResources, that.nodeResources) && Objects.equals(nodeUnassignedResources, that.nodeUnassignedResources) && Objects.equals(assignedPods, that.assignedPods) && Objects.equals(v1Node, that.v1Node);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, resourcePool, serverGroup, deployed, ipAddress, nodeResources, nodeUnassignedResources, assignedPods, v1Node);
    }

    @Override
    public String toString() {
        return "EmbeddedKubeNode{" +
                "name='" + name + '\'' +
                ", resourcePool='" + resourcePool + '\'' +
                ", serverGroup=" + serverGroup +
                ", deployed=" + deployed +
                ", ipAddress='" + ipAddress + '\'' +
                ", nodeResources=" + nodeResources +
                ", nodeUnassignedResources=" + nodeUnassignedResources +
                ", assignedPodsCount=" + assignedPods.size() +
                ", v1Node=" + v1Node +
                '}';
    }

    public String toStringCompact() {
        return "EmbeddedKubeNode{" +
                "name='" + name + '\'' +
                ", resourcePool='" + resourcePool + '\'' +
                ", serverGroup=" + serverGroup.getName() +
                ", zone=" + zone +
                ", deployed=" + deployed +
                ", ipAddress='" + ipAddress + '\'' +
                ", nodeUnassignedResources=" + nodeUnassignedResources +
                ", assignedPodsCount=" + assignedPods.size() +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder()
                .withName(name)
                .withResourcePool(resourcePool)
                .withServerGroup(serverGroup)
                .withZone(zone)
                .withDeployed(deployed)
                .withIpAddress(ipAddress)
                .withNodeResources(nodeResources)
                .withAssignedPods(assignedPods);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String name;
        private EmbeddedServerGroup serverGroup;
        private boolean deployed;
        private String ipAddress;
        private ResourceDimension nodeResources;
        private Map<String, V1Pod> assignedPods = new HashMap<>();
        private String resourcePool;
        private String zone;

        private Builder() {
        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withResourcePool(String resourcePool) {
            this.resourcePool = resourcePool;
            return this;
        }

        public Builder withServerGroup(EmbeddedServerGroup serverGroup) {
            this.serverGroup = serverGroup;
            return this;
        }

        public Builder withZone(String zone) {
            this.zone = zone;
            return this;
        }

        public Builder withDeployed(boolean deployed) {
            this.deployed = deployed;
            return this;
        }

        public Builder withIpAddress(String ipAddress) {
            this.ipAddress = ipAddress;
            return this;
        }

        public Builder withNodeResources(ResourceDimension nodeResources) {
            this.nodeResources = nodeResources;
            return this;
        }

        public Builder withAssignedPods(Map<String, V1Pod> assignedPods) {
            this.assignedPods = new HashMap<>(assignedPods);
            return this;
        }

        public Builder withPod(V1Pod pod) {
            assignedPods.put(pod.getMetadata().getName(), pod);
            return this;
        }

        public EmbeddedKubeNode build() {
            Preconditions.checkNotNull(name, "Node name not set");
            Preconditions.checkNotNull(serverGroup, "Server group name not set");
            Preconditions.checkNotNull(ipAddress, "Ip address not set");
            Preconditions.checkNotNull(nodeResources, "Node resources not set");

            if (zone == null) {
                int zoneCount = serverGroup.getZones().size();
                int idx = Math.min((int) Math.round(Math.random() * zoneCount), zoneCount - 1);
                zone = new ArrayList<>(serverGroup.getZones()).get(idx);
            }

            return new EmbeddedKubeNode(name, resourcePool, serverGroup, zone, deployed, ipAddress, nodeResources, buildNodeUnassignedResources(), assignedPods);
        }

        private ResourceDimension buildNodeUnassignedResources() {
            ResourceDimension podsTotal = ResourceDimension.empty();
            for (V1Pod pod : assignedPods.values()) {
                podsTotal = ResourceDimensions.add(podsTotal, fromPodToResourceDimension(pod));
            }
            return ResourceDimensions.subtractPositive(nodeResources, podsTotal);
        }
    }
}
