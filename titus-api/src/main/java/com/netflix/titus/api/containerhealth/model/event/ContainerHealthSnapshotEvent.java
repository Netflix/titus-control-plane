package com.netflix.titus.api.containerhealth.model.event;

import java.util.List;
import java.util.Objects;

import com.netflix.titus.api.containerhealth.model.ContainerHealthStatus;

public class ContainerHealthSnapshotEvent extends ContainerHealthEvent {

    private final List<ContainerHealthStatus> snapshot;

    public ContainerHealthSnapshotEvent(List<ContainerHealthStatus> snapshot) {
        this.snapshot = snapshot;
    }

    public List<ContainerHealthStatus> getSnapshot() {
        return snapshot;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ContainerHealthSnapshotEvent that = (ContainerHealthSnapshotEvent) o;
        return Objects.equals(snapshot, that.snapshot);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshot);
    }

    @Override
    public String toString() {
        return "ContainerHealthSnapshotEvent{" +
                "snapshot=" + snapshot +
                '}';
    }
}
