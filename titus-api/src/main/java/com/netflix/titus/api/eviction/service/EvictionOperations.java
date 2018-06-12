package com.netflix.titus.api.eviction.service;

import com.netflix.titus.api.eviction.model.EvictionQuota;
import com.netflix.titus.api.eviction.model.SystemDisruptionBudget;
import com.netflix.titus.api.eviction.model.event.EvictionEvent;
import com.netflix.titus.api.model.Tier;
import rx.Completable;
import rx.Observable;

public interface EvictionOperations {

    SystemDisruptionBudget getGlobalDisruptionBudget();

    SystemDisruptionBudget getTierDisruptionBudget(Tier tier);

    SystemDisruptionBudget getCapacityGroupDisruptionBudget(String capacityGroupName);

    EvictionQuota getGlobalEvictionQuota();

    EvictionQuota getTierEvictionQuota(Tier tier);

    EvictionQuota getCapacityGroupEvictionQuota(String capacityGroupName);

    Completable terminateTask(String taskId, String reason);

    Observable<EvictionEvent> events(boolean includeSnapshot);
}
