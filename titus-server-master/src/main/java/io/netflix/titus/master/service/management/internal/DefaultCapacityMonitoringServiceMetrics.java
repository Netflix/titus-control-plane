/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.service.management.internal;

import java.util.EnumMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import io.netflix.titus.api.model.ResourceDimension;
import io.netflix.titus.api.model.Tier;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.common.util.spectator.ExecutionMetrics;
import io.netflix.titus.master.MetricConstants;
import io.netflix.titus.master.service.management.CapacityGuaranteeStrategy.CapacityAllocations;

import static io.netflix.titus.common.util.CollectionsExt.asSet;

class DefaultCapacityMonitoringServiceMetrics {

    private enum ResourceType {Cpu, Memory, Disk, Network}

    private static Set<Tier> ALL_TIERS = asSet(Tier.values());

    private final ExecutionMetrics executionMetrics;
    private final EnumMap<Tier, EnumMap<ResourceType, AtomicLong>> resourceShortages;

    DefaultCapacityMonitoringServiceMetrics(Registry registry) {
        this.executionMetrics = new ExecutionMetrics(MetricConstants.METRIC_CAPACITY_MANAGEMENT + "monitor.capacityUpdates",
                DefaultCapacityMonitoringService.class, registry);
        this.resourceShortages = buildResourceShortageMap(registry);
    }

    ExecutionMetrics getUpdateExecutionMetrics() {
        return executionMetrics;
    }

    void recordResourceShortage(CapacityAllocations allocations) {
        Set<Tier> tiersWithShortage = allocations.getTiersWithResourceShortage();
        Set<Tier> okTiers = CollectionsExt.copyAndRemove(ALL_TIERS, tiersWithShortage);

        // Shortage
        for (Tier tier : tiersWithShortage) {
            ResourceDimension shortage = allocations.getResourceShortage(tier);
            EnumMap<ResourceType, AtomicLong> resourceGauges = resourceShortages.get(tier);

            resourceGauges.get(ResourceType.Cpu).set((int) Math.ceil(shortage.getCpu()));
            resourceGauges.get(ResourceType.Memory).set(shortage.getMemoryMB());
            resourceGauges.get(ResourceType.Disk).set(shortage.getDiskMB());
            resourceGauges.get(ResourceType.Network).set(shortage.getNetworkMbs());
        }

        // OK tiers
        for (Tier tier : okTiers) {
            EnumMap<ResourceType, AtomicLong> resourceGauges = resourceShortages.get(tier);

            resourceGauges.get(ResourceType.Cpu).set(0);
            resourceGauges.get(ResourceType.Memory).set(0);
            resourceGauges.get(ResourceType.Disk).set(0);
            resourceGauges.get(ResourceType.Network).set(0);
        }
    }

    private EnumMap<Tier, EnumMap<ResourceType, AtomicLong>> buildResourceShortageMap(Registry registry) {
        EnumMap<Tier, EnumMap<ResourceType, AtomicLong>> result = new EnumMap<>(Tier.class);
        for (Tier tier : Tier.values()) {
            EnumMap<ResourceType, AtomicLong> resourceMap = new EnumMap<>(ResourceType.class);
            result.put(tier, resourceMap);

            Id tierId = registry.createId(MetricConstants.METRIC_CAPACITY_MANAGEMENT + "monitor.capacityShortage", "tier", tier.name());

            for (ResourceType resourceType : ResourceType.values()) {
                resourceMap.put(
                        resourceType,
                        registry.gauge(tierId.withTag("resourceType", resourceType.name()), new AtomicLong())
                );
            }
        }
        return result;
    }
}
