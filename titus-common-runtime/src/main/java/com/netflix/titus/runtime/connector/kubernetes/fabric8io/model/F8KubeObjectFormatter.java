/*
 * Copyright 2022 Netflix, Inc.
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

package com.netflix.titus.runtime.connector.kubernetes.fabric8io.model;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodStatus;

public class F8KubeObjectFormatter {

    public static String formatPodEssentials(Pod pod) {
        try {
            return formatPodEssentialsInternal(pod);
        } catch (Exception e) {
            return "pod formatting error: " + e.getMessage();
        }
    }

    private static String formatPodEssentialsInternal(Pod pod) {
        StringBuilder builder = new StringBuilder("{");

        appendMetadata(builder, pod.getMetadata());

        PodSpec spec = pod.getSpec();
        if (spec != null) {
            builder.append(", nodeName=").append(spec.getNodeName());
        }

        PodStatus status = pod.getStatus();
        if (status != null) {
            builder.append(", phase=").append(status.getPhase());
            builder.append(", reason=").append(status.getReason());
        }

        builder.append("}");
        return builder.toString();
    }

    private static void appendMetadata(StringBuilder builder, ObjectMeta metadata) {
        if (metadata != null) {
            builder.append("name=").append(metadata.getName());
            builder.append(", labels=").append(metadata.getLabels());
        } else {
            builder.append("name=").append("<no_metadata>");
        }
    }
}
