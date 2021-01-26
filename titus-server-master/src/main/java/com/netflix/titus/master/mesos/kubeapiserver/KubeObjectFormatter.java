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

package com.netflix.titus.master.mesos.kubeapiserver;

import java.util.List;
import java.util.Map;

import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1NodeSpec;
import io.kubernetes.client.openapi.models.V1NodeStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodStatus;
import io.kubernetes.client.openapi.models.V1Taint;

/**
 * Helper functions to build compact representations of Kube objects suitable for logging.
 */
public final class KubeObjectFormatter {

    public static String formatPodEssentials(V1Pod pod) {
        StringBuilder builder = new StringBuilder("{");

        appendMetadata(builder, pod.getMetadata());

        V1PodSpec spec = pod.getSpec();
        if (spec != null) {
            builder.append(", nodeName=").append(spec.getNodeName());
        }

        V1PodStatus status = pod.getStatus();
        if (status != null) {
            builder.append(", phase=").append(status.getPhase());
            builder.append(", reason=").append(status.getReason());
        }

        builder.append("}");
        return builder.toString();
    }

    public static String formatNodeEssentials(V1Node node) {
        StringBuilder builder = new StringBuilder("{");

        appendMetadata(builder, node.getMetadata());

        V1NodeSpec spec = node.getSpec();
        if (spec != null) {
            List<V1Taint> taints = spec.getTaints();
            builder.append(", taints=");
            if (taints == null) {
                builder.append("[]");
            } else {
                builder.append("[");
                taints.forEach(taint -> {
                    builder.append("{");
                    builder.append("key=").append(taint.getKey()).append(", ");
                    builder.append("value=").append(taint.getValue()).append(", ");
                    builder.append("effect=").append(taint.getEffect());
                });
                builder.append("]");
            }
        }

        V1NodeStatus status = node.getStatus();
        if (status != null) {
            builder.append(", phase=").append(status.getPhase());

            Map<String, Quantity> allocatable = status.getAllocatable();
            if (allocatable != null) {
                builder.append(", allocatableResources={");
                allocatable.forEach((key, value) -> builder.append(key).append("=").append(value.getNumber().toString()).append(", "));
                builder.setLength(builder.length() - 2);
                builder.append("}");
            }
        }

        builder.append("}");
        return builder.toString();
    }

    private static void appendMetadata(StringBuilder builder, V1ObjectMeta metadata) {
        if (metadata != null) {
            builder.append("name=").append(metadata.getName());
            builder.append(", labels=").append(metadata.getLabels());
        } else {
            builder.append("name=").append("<no_metadata>");
        }
    }
}
