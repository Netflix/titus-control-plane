package com.netflix.titus.api.containerhealth.model.event;

public class ContainerHealthSnapshotEvent extends ContainerHealthEvent {

    private static final ContainerHealthSnapshotEvent INSTANCE = new ContainerHealthSnapshotEvent();

    public static ContainerHealthSnapshotEvent newInstance() {
        return INSTANCE;
    }
}
