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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.master.model.ResourceDimensions;
import com.netflix.titus.runtime.kubernetes.KubeConstants;
import io.kubernetes.client.openapi.models.V1NodeSelector;
import io.kubernetes.client.openapi.models.V1NodeSelectorRequirement;
import io.kubernetes.client.openapi.models.V1NodeSelectorTerm;
import io.kubernetes.client.openapi.models.V1Pod;

class EmbeddedKubeScheduler {

    private final EmbeddedKubeCluster embeddedKubeCluster;

    EmbeddedKubeScheduler(EmbeddedKubeCluster embeddedKubeCluster) {
        this.embeddedKubeCluster = embeddedKubeCluster;
    }

    void doSchedule() {
        Map<String, V1Pod> pods = embeddedKubeCluster.getPods();
        pods.forEach((name, pod) -> {
            if (pod.getStatus() == null || pod.getStatus().getHostIP() == null) {
                trySchedulePod(pod);
            }
        });
    }

    private void trySchedulePod(V1Pod pod) {
        Set<String> zoneConstraints = getZonesOfPod(pod);
        Map<String, EmbeddedKubeNode> nodes = getNodesOf(pod, zoneConstraints);
        ResourceDimension podResources = EmbeddedKubeUtil.fromPodToResourceDimension(pod);
        for (EmbeddedKubeNode node : nodes.values()) {
            ResourceDimension unassignedResources = node.getUnassignedResources();
            if (unassignedResources.equals(podResources) || ResourceDimensions.isBigger(unassignedResources, podResources)) {
                embeddedKubeCluster.assignPodToNode(pod.getMetadata().getName(), node);
                return;
            }
        }
    }

    private List<String> findNodeSelectorTerm(V1Pod pod, String key) {
        if (pod.getSpec() == null
                || pod.getSpec().getAffinity() == null
                || pod.getSpec().getAffinity().getNodeAffinity() == null
                || pod.getSpec().getAffinity().getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution() == null) {
            return null;
        }
        V1NodeSelector nodeSelector = pod.getSpec().getAffinity().getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution();
        for (V1NodeSelectorTerm term : nodeSelector.getNodeSelectorTerms()) {
            for (V1NodeSelectorRequirement matcher : term.getMatchExpressions()) {
                if (matcher.getKey().equals(key)) {
                    return matcher.getValues();
                }
            }
        }
        return null;
    }

    public String getResourcePoolOfPod(V1Pod pod) {
        return findNodeSelectorTerm(pod, KubeConstants.NODE_LABEL_RESOURCE_POOL).get(0);
    }

    public Set<String> getZonesOfPod(V1Pod pod) {
        List<String> zones = findNodeSelectorTerm(pod, KubeConstants.NODE_LABEL_ZONE);
        return CollectionsExt.isNullOrEmpty(zones) ? Collections.emptySet() : new HashSet<>(zones);
    }

    private Map<String, EmbeddedKubeNode> getNodesOf(V1Pod pod, Set<String> zoneConstraints) {
        String resourcePool = getResourcePoolOfPod(pod);
        if (StringExt.isEmpty(resourcePool)) {
            return Collections.emptyMap();
        }
        Map<String, EmbeddedKubeNode> poolNodes = new HashMap<>(embeddedKubeCluster.getFleet().getNodes());
        poolNodes.values().removeIf(node -> !isInResourcePool(node, resourcePool) || !isInZone(node, zoneConstraints));

        return poolNodes;
    }

    private boolean isInResourcePool(EmbeddedKubeNode node, String resourcePool) {
        return Objects.equals(node.getResourcePool(), resourcePool);
    }

    private boolean isInZone(EmbeddedKubeNode node, Set<String> zoneConstraints) {
        return zoneConstraints.isEmpty() || zoneConstraints.contains(node.getZone());
    }
}
