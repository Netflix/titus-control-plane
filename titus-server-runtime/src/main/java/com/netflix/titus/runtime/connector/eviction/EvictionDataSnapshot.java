package com.netflix.titus.runtime.connector.eviction;

import java.util.Map;
import java.util.Optional;

import com.netflix.titus.api.eviction.model.EvictionQuota;
import com.netflix.titus.api.eviction.model.SystemDisruptionBudget;
import com.netflix.titus.api.model.Reference.TierReference;
import com.netflix.titus.api.model.Tier;

import static com.netflix.titus.common.util.CollectionsExt.copyAndAdd;

public class EvictionDataSnapshot {

    private final SystemDisruptionBudget globalDisruptionBudget;
    private final Map<Tier, SystemDisruptionBudget> tierSystemDisruptionBudges;
    private final Map<String, SystemDisruptionBudget> capacityGroupSystemDisruptionBudges;
    private final EvictionQuota globalEvictionQuota;
    private final Map<Tier, EvictionQuota> tierEvictionQuotas;
    private final Map<String, EvictionQuota> capacityGroupEvictionQuotas;

    public EvictionDataSnapshot(SystemDisruptionBudget globalDisruptionBudget,
                                Map<Tier, SystemDisruptionBudget> tierSystemDisruptionBudges,
                                Map<String, SystemDisruptionBudget> capacityGroupSystemDisruptionBudges,
                                EvictionQuota globalEvictionQuota,
                                Map<Tier, EvictionQuota> tierEvictionQuotas,
                                Map<String, EvictionQuota> capacityGroupEvictionQuotas) {
        this.globalDisruptionBudget = globalDisruptionBudget;
        this.tierSystemDisruptionBudges = tierSystemDisruptionBudges;
        this.capacityGroupSystemDisruptionBudges = capacityGroupSystemDisruptionBudges;
        this.globalEvictionQuota = globalEvictionQuota;
        this.tierEvictionQuotas = tierEvictionQuotas;
        this.capacityGroupEvictionQuotas = capacityGroupEvictionQuotas;
    }

    public SystemDisruptionBudget getGlobalDisruptionBudget() {
        return globalDisruptionBudget;
    }

    public SystemDisruptionBudget getTierDisruptionBudget(Tier tier) {
        return tierSystemDisruptionBudges.get(tier);
    }

    public Optional<SystemDisruptionBudget> findCapacityGroupDisruptionBudget(String capacityGroupName) {
        return Optional.ofNullable(capacityGroupSystemDisruptionBudges.get(capacityGroupName));
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
                        this.tierSystemDisruptionBudges,
                        this.capacityGroupSystemDisruptionBudges,
                        this.globalEvictionQuota,
                        this.tierEvictionQuotas,
                        this.capacityGroupEvictionQuotas
                ));
            case Tier:
                return Optional.of(new EvictionDataSnapshot(
                        this.globalDisruptionBudget,
                        copyAndAdd(this.tierSystemDisruptionBudges, ((TierReference) systemDisruptionBudget.getReference()).getTier(), systemDisruptionBudget),
                        this.capacityGroupSystemDisruptionBudges,
                        this.globalEvictionQuota,
                        this.tierEvictionQuotas,
                        this.capacityGroupEvictionQuotas
                ));
            case CapacityGroup:
                return Optional.of(new EvictionDataSnapshot(
                        this.globalDisruptionBudget,
                        this.tierSystemDisruptionBudges,
                        copyAndAdd(this.capacityGroupSystemDisruptionBudges, systemDisruptionBudget.getReference().getName(), systemDisruptionBudget),
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
                        this.tierSystemDisruptionBudges,
                        this.capacityGroupSystemDisruptionBudges,
                        quota,
                        this.tierEvictionQuotas,
                        this.capacityGroupEvictionQuotas
                ));
            case Tier:
                return Optional.of(new EvictionDataSnapshot(
                        this.globalDisruptionBudget,
                        this.tierSystemDisruptionBudges,
                        this.capacityGroupSystemDisruptionBudges,
                        this.globalEvictionQuota,
                        copyAndAdd(this.tierEvictionQuotas, ((TierReference) quota.getReference()).getTier(), quota),
                        this.capacityGroupEvictionQuotas
                ));
            case CapacityGroup:
                return Optional.of(new EvictionDataSnapshot(
                        this.globalDisruptionBudget,
                        this.tierSystemDisruptionBudges,
                        this.capacityGroupSystemDisruptionBudges,
                        this.globalEvictionQuota,
                        this.tierEvictionQuotas,
                        copyAndAdd(this.capacityGroupEvictionQuotas, quota.getReference().getName(), quota)
                ));
        }
        return Optional.empty();
    }
}
