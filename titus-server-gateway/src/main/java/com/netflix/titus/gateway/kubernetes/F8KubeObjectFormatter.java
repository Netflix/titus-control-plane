package com.netflix.titus.gateway.kubernetes;

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
