package com.netflix.titus.api.containerhealth.model.event;

import java.util.List;

import com.netflix.titus.api.containerhealth.model.ContainerHealthStatus;

public abstract class ContainerHealthEvent {

    public static ContainerHealthUpdateEvent healthChanged(ContainerHealthStatus status) {
        return new ContainerHealthUpdateEvent(status);
    }

    public static ContainerHealthSnapshotEvent snapshot(List<ContainerHealthStatus> statuses) {
        return new ContainerHealthSnapshotEvent(statuses);
    }
}
