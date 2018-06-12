package com.netflix.titus.runtime.connector.common.replicator;

import rx.Observable;

/**
 * An auxiliary interface used within the connector components only to deal with the remote Rx event streams.
 */
public interface ReplicatorEventStream<D> {

    long LATENCY_REPORT_INTERVAL_MS = 1_000;

    Observable<ReplicatorEvent<D>> connect();

    class ReplicatorEvent<D> {
        private final D data;
        private final long lastUpdateTime;

        public ReplicatorEvent(D data, long lastUpdateTime) {
            this.data = data;
            this.lastUpdateTime = lastUpdateTime;
        }

        public D getData() {
            return data;
        }

        public long getLastUpdateTime() {
            return lastUpdateTime;
        }
    }
}
