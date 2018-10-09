package com.netflix.titus.api.eviction.service;

import rx.Completable;

public interface EvictionOperations extends ReadOnlyEvictionOperations {

    Completable terminateTask(String taskId, String reason);

}
