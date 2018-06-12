package com.netflix.titus.api.eviction.model.event;

import com.netflix.titus.api.eviction.model.EvictionQuota;
import com.netflix.titus.api.eviction.model.SystemDisruptionBudget;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.model.Reference;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.util.limiter.tokenbucket.TokenBucket;

public abstract class EvictionEvent {

    public static EvictionEvent newSnapshotEndEvent() {
        return EvictionSnapshotEndEvent.getInstance();
    }

    public static EvictionEvent newSystemDisruptionBudgetEvent(SystemDisruptionBudget disruptionBudget) {
        return new SystemDisruptionBudgetUpdateEvent(disruptionBudget);
    }

    public static EvictionEvent newQuotaEvent(EvictionQuota evictionQuota) {
        return new EvictionQuotaEvent(evictionQuota);
    }

    public static EvictionQuotaEvent newGlobalQuotaEvent(TokenBucket tokenBucket) {
        return new EvictionQuotaEvent(new EvictionQuota(Reference.global(), tokenBucket.getNumberOfTokens()));
    }

    public static EvictionEvent newTierQuotaEvent(Tier tier, TokenBucket tokenBuckets) {
        return new EvictionQuotaEvent(new EvictionQuota(Reference.tier(tier), tokenBuckets.getNumberOfTokens()));
    }

    public static EvictionEvent newCapacityGroupQuotaEvent(ApplicationSLA capacityGroup, TokenBucket tokenBucket) {
        return new EvictionQuotaEvent(new EvictionQuota(Reference.capacityGroup(capacityGroup.getAppName()), tokenBucket.getNumberOfTokens()));
    }

    public static EvictionEvent newTaskTerminationEvent(String taskId, boolean approved) {
        return new TaskTerminationEvent(taskId, approved);
    }
}
