package com.netflix.titus.runtime.connector.common.replicator;

import rx.Observable;

/**
 * Data replicator from a remote service.
 */
public interface DataReplicator<D> {
    /**
     * Get the latest known version of the data.
     */
    D getCurrent();

    /**
     * Returns the number of milliseconds since the last data refresh time.
     */
    long getStalenessMs();

    /**
     * Emits periodically the number of milliseconds since the last data refresh time. Emits an error when the
     * cache refresh process fails, and cannot resume.
     */
    Observable<Long> observeDataStalenessMs();
}
