package com.netflix.titus.api.eviction.service;

import java.util.Optional;

import com.netflix.titus.api.eviction.model.EvictionQuota;
import com.netflix.titus.api.eviction.model.SystemDisruptionBudget;
import com.netflix.titus.api.eviction.model.event.EvictionEvent;
import com.netflix.titus.api.model.Tier;
import rx.Observable;

public interface ReadOnlyEvictionOperations {

    SystemDisruptionBudget getGlobalDisruptionBudget();

    SystemDisruptionBudget getTierDisruptionBudget(Tier tier);

    SystemDisruptionBudget getCapacityGroupDisruptionBudget(String capacityGroupName);

    EvictionQuota getGlobalEvictionQuota();

    EvictionQuota getTierEvictionQuota(Tier tier);

    EvictionQuota getCapacityGroupEvictionQuota(String capacityGroupName);

    Optional<EvictionQuota> findJobEvictionQuota(String jobId);

    Observable<EvictionEvent> events(boolean includeSnapshot);
}
