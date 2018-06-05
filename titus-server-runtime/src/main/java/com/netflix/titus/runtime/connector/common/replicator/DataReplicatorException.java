package com.netflix.titus.runtime.connector.common.replicator;

import java.util.Optional;

class DataReplicatorException extends RuntimeException {

    private final Optional<ReplicatorEventStream.ReplicatorEvent<?>> lastCacheEvent;

    DataReplicatorException(Optional<ReplicatorEventStream.ReplicatorEvent<?>> lastCacheEvent, Throwable cause) {
        super(cause);
        this.lastCacheEvent = lastCacheEvent;
    }

    Optional<ReplicatorEventStream.ReplicatorEvent<?>> getLastCacheEvent() {
        return lastCacheEvent;
    }
}
