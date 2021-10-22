package com.netflix.titus.gateway.kubernetes;

import java.util.Objects;
import java.util.Optional;

import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.Pod;

public class PodDeletedEvent extends PodEvent{

    private final boolean deletedFinalStateUnknown;
    private final Optional<Node> node;

    PodDeletedEvent(Pod pod, boolean deletedFinalStateUnknown, Optional<Node> node) {
        super(pod);
        this.deletedFinalStateUnknown = deletedFinalStateUnknown;
        this.node = node;
    }

    public boolean isDeletedFinalStateUnknown() {
        return deletedFinalStateUnknown;
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
        PodDeletedEvent that = (PodDeletedEvent) o;
        return deletedFinalStateUnknown == that.deletedFinalStateUnknown &&
                Objects.equals(node, that.node);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), deletedFinalStateUnknown, node);
    }

    @Override
    public String toString() {
        return "PodDeletedEvent{" +
                "taskId='" + taskId + '\'' +
                ", sequenceNumber=" + sequenceNumber +
                ", pod=" + F8KubeObjectFormatter.formatPodEssentials(pod) +
                ", deletedFinalStateUnknown=" + deletedFinalStateUnknown +
                ", node=" + node.map(n -> n.getMetadata().getName()).orElse("<not_assigned>") +
                '}';
    }

}
