package com.netflix.titus.runtime.connector.jobmanager.cache;

import com.netflix.titus.runtime.connector.jobmanager.JobCache;
import rx.Observable;

public interface JobStreamCache {

    long LATENCY_REPORT_INTERVAL_MS = 1_000;

    Observable<CacheEvent> connect();

    class CacheEvent {
        private final JobCache cache;
        private final long lastUpdateTime;

        CacheEvent(JobCache cache, long lastUpdateTime) {
            this.cache = cache;
            this.lastUpdateTime = lastUpdateTime;
        }

        JobCache getCache() {
            return cache;
        }

        long getLastUpdateTime() {
            return lastUpdateTime;
        }
    }
}
