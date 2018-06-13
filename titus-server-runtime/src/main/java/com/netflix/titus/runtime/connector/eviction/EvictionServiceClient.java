package com.netflix.titus.runtime.connector.eviction;

import com.netflix.titus.api.eviction.model.EvictionQuota;
import com.netflix.titus.api.eviction.model.SystemDisruptionBudget;
import com.netflix.titus.api.eviction.model.event.EvictionEvent;
import com.netflix.titus.api.model.reference.Reference;
import rx.Completable;
import rx.Observable;

public interface EvictionServiceClient {

    Observable<SystemDisruptionBudget> getDisruptionBudget(Reference reference);

    Observable<EvictionQuota> getEvictionQuota(Reference reference);

    Completable terminateTask(String taskId, String reason);

    Observable<EvictionEvent> observeEvents(boolean includeSnapshot);
}
