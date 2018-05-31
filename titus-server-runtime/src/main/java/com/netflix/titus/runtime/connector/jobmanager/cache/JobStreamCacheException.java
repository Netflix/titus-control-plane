package com.netflix.titus.runtime.connector.jobmanager.cache;

import java.util.Optional;

class JobStreamCacheException extends RuntimeException {

    private final Optional<JobStreamCache.CacheEvent> lastCacheEvent;

    JobStreamCacheException(Optional<JobStreamCache.CacheEvent> lastCacheEvent, Throwable cause) {
        super(cause);
        this.lastCacheEvent = lastCacheEvent;
    }

    Optional<JobStreamCache.CacheEvent> getLastCacheEvent() {
        return lastCacheEvent;
    }
}
