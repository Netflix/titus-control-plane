package com.netflix.titus.runtime.connector.jobmanager;

import rx.Observable;

public interface JobCacheResolver {

    /**
     * Get the latest job cache version.
     */
    JobCache getCurrent();

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
