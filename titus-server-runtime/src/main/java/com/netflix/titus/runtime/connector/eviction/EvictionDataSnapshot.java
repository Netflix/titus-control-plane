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
import com.netflix.titus.api.eviction.model.SystemDisruptionBudget;
import com.netflix.titus.api.model.reference.TierReference;
import com.netflix.titus.api.model.Tier;

import static com.netflix.titus.common.util.CollectionsExt.copyAndAdd;

public class EvictionDataSnapshot {

    private final SystemDisruptionBudget globalDisruptionBudget;
    private final Map<Tier, SystemDisruptionBudget> tierSystemDisruptionBudgets;
    private final Map<String, SystemDisruptionBudget> capacityGroupSystemDisruptionBudgets;
    private final EvictionQuota globalEvictionQuota;
    private final Map<Tier, EvictionQuota> tierEvictionQuotas;
    private final Map<String, EvictionQuota> capacityGroupEvictionQuotas;

    public EvictionDataSnapshot(SystemDisruptionBudget globalDisruptionBudget,
                                Map<Tier, SystemDisruptionBudget> tierSystemDisruptionBudgets,
                                Map<String, SystemDisruptionBudget> capacityGroupSystemDisruptionBudgets,
                                EvictionQuota globalEvictionQuota,
                                Map<Tier, EvictionQuota> tierEvictionQuotas,
                                Map<String, EvictionQuota> capacityGroupEvictionQuotas) {
        this.globalDisruptionBudget = globalDisruptionBudget;
        this.tierSystemDisruptionBudgets = tierSystemDisruptionBudgets;
        this.capacityGroupSystemDisruptionBudgets = capacityGroupSystemDisruptionBudgets;
        this.globalEvictionQuota = globalEvictionQuota;
        this.tierEvictionQuotas = tierEvictionQuotas;
        this.capacityGroupEvictionQuotas = capacityGroupEvictionQuotas;
    }

    public SystemDisruptionBudget getGlobalDisruptionBudget() {
        return globalDisruptionBudget;
    }

    public SystemDisruptionBudget getTierDisruptionBudget(Tier tier) {
        return tierSystemDisruptionBudgets.get(tier);
    }

    public Optional<SystemDisruptionBudget> findCapacityGroupDisruptionBudget(String capacityGroupName) {
        return Optional.ofNullable(capacityGroupSystemDisruptionBudgets.get(capacityGroupName));
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

    public Optional<EvictionDataSnapshot> updateSystemDisruptionBudget(SystemDisruptionBudget systemDisruptionBudget) {
        switch (systemDisruptionBudget.getReference().getLevel()) {
            case Global:
                return Optional.of(new EvictionDataSnapshot(
                        systemDisruptionBudget,
                        this.tierSystemDisruptionBudgets,
                        this.capacityGroupSystemDisruptionBudgets,
                        this.globalEvictionQuota,
                        this.tierEvictionQuotas,
                        this.capacityGroupEvictionQuotas
                ));
            case Tier:
                return Optional.of(new EvictionDataSnapshot(
                        this.globalDisruptionBudget,
                        copyAndAdd(this.tierSystemDisruptionBudgets, ((TierReference) systemDisruptionBudget.getReference()).getTier(), systemDisruptionBudget),
                        this.capacityGroupSystemDisruptionBudgets,
                        this.globalEvictionQuota,
                        this.tierEvictionQuotas,
                        this.capacityGroupEvictionQuotas
                ));
            case CapacityGroup:
                return Optional.of(new EvictionDataSnapshot(
                        this.globalDisruptionBudget,
                        this.tierSystemDisruptionBudgets,
                        copyAndAdd(this.capacityGroupSystemDisruptionBudgets, systemDisruptionBudget.getReference().getName(), systemDisruptionBudget),
                        this.globalEvictionQuota,
                        this.tierEvictionQuotas,
                        this.capacityGroupEvictionQuotas
                ));
        }
        return Optional.empty();
    }

    public Optional<EvictionDataSnapshot> updateEvictionQuota(EvictionQuota quota) {
        switch (quota.getReference().getLevel()) {
            case Global:
                return Optional.of(new EvictionDataSnapshot(
                        this.globalDisruptionBudget,
                        this.tierSystemDisruptionBudgets,
                        this.capacityGroupSystemDisruptionBudgets,
                        quota,
                        this.tierEvictionQuotas,
                        this.capacityGroupEvictionQuotas
                ));
            case Tier:
                return Optional.of(new EvictionDataSnapshot(
                        this.globalDisruptionBudget,
                        this.tierSystemDisruptionBudgets,
                        this.capacityGroupSystemDisruptionBudgets,
                        this.globalEvictionQuota,
                        copyAndAdd(this.tierEvictionQuotas, ((TierReference) quota.getReference()).getTier(), quota),
                        this.capacityGroupEvictionQuotas
                ));
            case CapacityGroup:
                return Optional.of(new EvictionDataSnapshot(
                        this.globalDisruptionBudget,
                        this.tierSystemDisruptionBudgets,
                        this.capacityGroupSystemDisruptionBudgets,
                        this.globalEvictionQuota,
                        this.tierEvictionQuotas,
                        copyAndAdd(this.capacityGroupEvictionQuotas, quota.getReference().getName(), quota)
                ));
        }
        return Optional.empty();
    }
}
