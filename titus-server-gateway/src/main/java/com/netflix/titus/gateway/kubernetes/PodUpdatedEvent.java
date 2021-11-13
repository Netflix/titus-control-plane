package com.netflix.titus.gateway.kubernetes;

import java.util.Objects;
import java.util.Optional;

import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.Pod;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1Pod;

public class PodUpdatedEvent extends PodEvent {

    private final Pod oldPod;
    private final Optional<Node> node;

    PodUpdatedEvent(Pod oldPod, Pod newPod, Optional<Node> node) {
        super(newPod);
        this.oldPod = oldPod;
        this.node = node;
    }

    public Pod getOldPod() {
        return oldPod;
    }

    public Optional<Node> getNode() {
        return node;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        PodUpdatedEvent that = (PodUpdatedEvent) o;
        return Objects.equals(oldPod, that.oldPod) &&
                Objects.equals(node, that.node);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), oldPod, node);
    }

    @Override
    public String toString() {
        return "PodUpdatedEvent{" +
                "taskId='" + taskId + '\'' +
                ", sequenceNumber=" + sequenceNumber +
                ", pod=" + F8KubeObjectFormatter.formatPodEssentials(pod) +
                ", oldPod=" + F8KubeObjectFormatter.formatPodEssentials(oldPod) +
                ", node=" + node.map(n -> n.getMetadata().getName()).orElse("<not_assigned>") +
                '}';
    }
}
