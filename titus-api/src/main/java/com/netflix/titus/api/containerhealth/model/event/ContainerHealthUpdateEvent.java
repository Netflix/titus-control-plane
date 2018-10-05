package com.netflix.titus.api.containerhealth.model.event;

import java.util.Objects;

import com.netflix.titus.api.containerhealth.model.ContainerHealthStatus;

public class ContainerHealthUpdateEvent extends ContainerHealthEvent {

    private final ContainerHealthStatus containerHealthStatus;

    public ContainerHealthUpdateEvent(ContainerHealthStatus containerHealthStatus) {
        this.containerHealthStatus = containerHealthStatus;
    }

    public ContainerHealthStatus getContainerHealthStatus() {
        return containerHealthStatus;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ContainerHealthUpdateEvent that = (ContainerHealthUpdateEvent) o;
        return Objects.equals(containerHealthStatus, that.containerHealthStatus);
    }

    @Override
    public int hashCode() {
        return Objects.hash(containerHealthStatus);
    }

    @Override
    public String toString() {
        return "ContainerHealthChangeEvent{" +
                "containerHealthStatus=" + containerHealthStatus +
                '}';
    }
}
