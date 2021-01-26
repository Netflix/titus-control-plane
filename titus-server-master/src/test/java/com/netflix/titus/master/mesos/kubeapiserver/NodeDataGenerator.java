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

package com.netflix.titus.master.mesos.kubeapiserver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.netflix.titus.common.util.CollectionsExt;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1NodeAddress;
import io.kubernetes.client.openapi.models.V1NodeSpec;
import io.kubernetes.client.openapi.models.V1NodeStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Taint;

public class NodeDataGenerator {

    public static final String NODE_NAME = "node1";

    @SafeVarargs
    public static V1Node newNode(String nodeName, Function<V1Node, V1Node>... transformers) {
        V1Node node = new V1Node()
                .metadata(new V1ObjectMeta().name(nodeName))
                .status(new V1NodeStatus());
        transform(node, transformers);
        return node;
    }

    @SafeVarargs
    public static V1Node newNode(Function<V1Node, V1Node>... transformers) {
        return newNode(NODE_NAME, transformers);
    }

    @SafeVarargs
    public static void transform(V1Node node, Function<V1Node, V1Node>... transformers) {
        for (Function<V1Node, V1Node> transformer : transformers) {
            transformer.apply(node);
        }
    }

    public static Function<V1Node, V1Node> andNodePhase(String phase) {
        return node -> {
            if (node.getStatus() == null) {
                node.status(new V1NodeStatus());
            }
            node.getStatus().phase(phase);
            return node;
        };
    }

    public static Function<V1Node, V1Node> andNodeAllocatableResources(int cpu, int memoryMB, int diskMB, int networkMbps) {
        return node -> {
            if (node.getStatus() == null) {
                node.status(new V1NodeStatus());
            }
            Map<String, Quantity> allocatable = new HashMap<>();
            allocatable.put("cpu", new Quantity(cpu + ""));
            allocatable.put("memory", new Quantity(memoryMB + "M"));
            allocatable.put("disk", new Quantity(diskMB + "M"));
            allocatable.put("network", new Quantity(networkMbps + "Mi"));
            node.getStatus().allocatable(allocatable);
            return node;
        };
    }

    public static V1Node andIpAddress(String ipAddress, V1Node node) {
        return andIpAddress(ipAddress).apply(node);
    }

    public static Function<V1Node, V1Node> andIpAddress(String ipAddress) {
        return node -> {
            if (node.getStatus() == null) {
                node.status(new V1NodeStatus());
            }
            node.getStatus().addresses(
                    Collections.singletonList(new V1NodeAddress().address(ipAddress).type(KubeUtil.TYPE_INTERNAL_IP))
            );
            return node;
        };
    }

    public static V1Node andNodeAnnotations(V1Node node, String... keyValuePairs) {
        return andNodeAnnotations(keyValuePairs).apply(node);
    }

    public static Function<V1Node, V1Node> andNodeLabels(String... keyValuePairs) {
        return node -> {
            Map<String, String> labels = CollectionsExt.copyAndAdd(
                    CollectionsExt.nonNull(node.getMetadata().getLabels()),
                    CollectionsExt.asMap(keyValuePairs)
            );
            node.getMetadata().labels(labels);
            return node;
        };
    }

    public static Function<V1Node, V1Node> andNodeAnnotations(String... keyValuePairs) {
        return node -> {
            Map<String, String> annotations = CollectionsExt.copyAndAdd(
                    CollectionsExt.nonNull(node.getMetadata().getAnnotations()),
                    CollectionsExt.asMap(keyValuePairs)
            );
            node.getMetadata().annotations(annotations);
            return node;
        };
    }

    public static Function<V1Node, V1Node> andTaint(String key, String value, String effect) {
        return node -> {
            if (node.getSpec() == null) {
                node.spec(new V1NodeSpec());
            }
            List<V1Taint> taints = node.getSpec().getTaints();
            if (taints == null) {
                node.getSpec().taints(taints = new ArrayList<>());
            }
            taints.add(new V1Taint().key(key).value(value).effect(effect));
            return node;
        };
    }
}
