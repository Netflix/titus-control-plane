package com.netflix.titus.api.eviction.model.event;

public class EvictionSnapshotEndEvent extends EvictionEvent {

    private static final EvictionSnapshotEndEvent INSTANCE = new EvictionSnapshotEndEvent();

    public static EvictionSnapshotEndEvent getInstance() {
        return INSTANCE;
    }
}
