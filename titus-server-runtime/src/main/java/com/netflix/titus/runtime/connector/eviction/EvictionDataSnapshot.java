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

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import com.netflix.titus.api.eviction.model.EvictionQuota;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.api.model.reference.TierReference;
import com.netflix.titus.common.util.CollectionsExt;

import static com.netflix.titus.common.util.CollectionsExt.copyAndAdd;

/**
 * TODO Removed job cleanup (not critical, as forced reconnects and the snapshot rebuild will do the work).
 */
public class EvictionDataSnapshot {

    private static final EvictionDataSnapshot EMPTY = new EvictionDataSnapshot(
            EvictionQuota.systemQuota(0),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
    );

    private final EvictionQuota systemEvictionQuota;
    private final Map<Tier, EvictionQuota> tierEvictionQuotas;
    private final Map<String, EvictionQuota> capacityGroupEvictionQuotas;
    private final Map<String, EvictionQuota> jobEvictionQuotas;

    public EvictionDataSnapshot(EvictionQuota systemEvictionQuota,
                                Map<Tier, EvictionQuota> tierEvictionQuotas,
                                Map<String, EvictionQuota> capacityGroupEvictionQuotas,
                                Map<String, EvictionQuota> jobEvictionQuotas) {
        this.systemEvictionQuota = systemEvictionQuota;
        this.tierEvictionQuotas = tierEvictionQuotas;
        this.capacityGroupEvictionQuotas = capacityGroupEvictionQuotas;
        this.jobEvictionQuotas = jobEvictionQuotas;
    }

    public EvictionQuota getSystemEvictionQuota() {
        return systemEvictionQuota;
    }

    public EvictionQuota getTierEvictionQuota(Tier tier) {
        return tierEvictionQuotas.get(tier);
    }

    public Optional<EvictionQuota> findCapacityGroupEvictionQuota(String capacityGroupName) {
        return Optional.ofNullable(capacityGroupEvictionQuotas.get(capacityGroupName));
    }

    public Optional<EvictionQuota> findJobEvictionQuota(String jobId) {
        return Optional.ofNullable(jobEvictionQuotas.get(jobId));
    }

    public Optional<EvictionDataSnapshot> updateEvictionQuota(EvictionQuota quota) {
        switch (quota.getReference().getLevel()) {
            case System:
                return Optional.of(new EvictionDataSnapshot(
                        quota,
                        this.tierEvictionQuotas,
                        this.capacityGroupEvictionQuotas,
                        jobEvictionQuotas));
            case Tier:
                return Optional.of(new EvictionDataSnapshot(
                        this.systemEvictionQuota,
                        copyAndAdd(this.tierEvictionQuotas, ((TierReference) quota.getReference()).getTier(), quota),
                        this.capacityGroupEvictionQuotas,
                        jobEvictionQuotas));
            case CapacityGroup:
                return Optional.of(new EvictionDataSnapshot(
                        this.systemEvictionQuota,
                        this.tierEvictionQuotas,
                        copyAndAdd(this.capacityGroupEvictionQuotas, quota.getReference().getName(), quota),
                        jobEvictionQuotas));
            case Job:
                return Optional.of(new EvictionDataSnapshot(
                        this.systemEvictionQuota,
                        this.tierEvictionQuotas,
                        this.capacityGroupEvictionQuotas,
                        CollectionsExt.copyAndAdd(jobEvictionQuotas, quota.getReference().getName(), quota)
                ));
        }
        return Optional.empty();
    }

    @Override
    public String toString() {
        return "EvictionDataSnapshot{" +
                "systemEvictionQuota=" + systemEvictionQuota +
                ", tierEvictionQuotas=" + tierEvictionQuotas +
                ", capacityGroupEvictionQuotas=" + capacityGroupEvictionQuotas +
                ", jobEvictionQuotas=" + jobEvictionQuotas +
                '}';
    }

    public static EvictionDataSnapshot empty() {
        return EMPTY;
    }
}
