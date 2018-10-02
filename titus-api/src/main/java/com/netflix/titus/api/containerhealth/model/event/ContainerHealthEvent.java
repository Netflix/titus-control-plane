package com.netflix.titus.api.containerhealth.model.event;

import com.netflix.titus.api.containerhealth.model.ContainerHealthStatus;

public abstract class ContainerHealthEvent {

    public static ContainerHealthChangeEvent healthChanged(ContainerHealthStatus status) {
        return new ContainerHealthChangeEvent(status);
    }
}
