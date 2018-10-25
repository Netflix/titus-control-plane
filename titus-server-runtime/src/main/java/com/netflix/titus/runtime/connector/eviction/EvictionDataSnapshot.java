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

package com.netflix.titus.runtime.connector.eviction;

import java.util.Map;
import java.util.Optional;

import com.netflix.titus.api.eviction.model.EvictionQuota;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.api.model.reference.TierReference;

import static com.netflix.titus.common.util.CollectionsExt.copyAndAdd;

public class EvictionDataSnapshot {

    private final EvictionQuota globalEvictionQuota;
    private final Map<Tier, EvictionQuota> tierEvictionQuotas;
    private final Map<String, EvictionQuota> capacityGroupEvictionQuotas;

    public EvictionDataSnapshot(EvictionQuota globalEvictionQuota,
                                Map<Tier, EvictionQuota> tierEvictionQuotas,
                                Map<String, EvictionQuota> capacityGroupEvictionQuotas) {
        this.globalEvictionQuota = globalEvictionQuota;
        this.tierEvictionQuotas = tierEvictionQuotas;
        this.capacityGroupEvictionQuotas = capacityGroupEvictionQuotas;
    }

    public EvictionQuota getGlobalEvictionQuota() {
        return globalEvictionQuota;
    }

    public EvictionQuota getTierEvictionQuota(Tier tier) {
        return tierEvictionQuotas.get(tier);
    }

    public Optional<EvictionQuota> findCapacityGroupEvictionQuota(String capacityGroupName) {
        return Optional.ofNullable(capacityGroupEvictionQuotas.get(capacityGroupName));
    }

    public Optional<EvictionDataSnapshot> updateEvictionQuota(EvictionQuota quota) {
        switch (quota.getReference().getLevel()) {
            case Global:
                return Optional.of(new EvictionDataSnapshot(
                        quota,
                        this.tierEvictionQuotas,
                        this.capacityGroupEvictionQuotas
                ));
            case Tier:
                return Optional.of(new EvictionDataSnapshot(
                        this.globalEvictionQuota,
                        copyAndAdd(this.tierEvictionQuotas, ((TierReference) quota.getReference()).getTier(), quota),
                        this.capacityGroupEvictionQuotas
                ));
            case CapacityGroup:
                return Optional.of(new EvictionDataSnapshot(
                        this.globalEvictionQuota,
                        this.tierEvictionQuotas,
                        copyAndAdd(this.capacityGroupEvictionQuotas, quota.getReference().getName(), quota)
                ));
        }
        return Optional.empty();
    }
}
