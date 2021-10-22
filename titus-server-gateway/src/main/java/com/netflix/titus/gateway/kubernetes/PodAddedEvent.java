package com.netflix.titus.gateway.kubernetes;

import io.fabric8.kubernetes.api.model.Pod;
import io.kubernetes.client.openapi.models.V1Pod;

public class PodAddedEvent extends PodEvent {
    PodAddedEvent(Pod pod) {
        super(pod);
    }

    @Override
    public String toString() {
        return "PodAddedEvent{" +
                "taskId=" + taskId +
                ", sequenceNumber=" + sequenceNumber +
                ", pod=" + F8KubeObjectFormatter.formatPodEssentials(pod) +
                '}';
    }
}
