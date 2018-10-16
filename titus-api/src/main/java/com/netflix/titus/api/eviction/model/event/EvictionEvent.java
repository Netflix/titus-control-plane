/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.titus.api.eviction.model.event;

import com.netflix.titus.api.eviction.model.EvictionQuota;
import com.netflix.titus.api.eviction.model.SystemDisruptionBudget;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.model.reference.Reference;
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
