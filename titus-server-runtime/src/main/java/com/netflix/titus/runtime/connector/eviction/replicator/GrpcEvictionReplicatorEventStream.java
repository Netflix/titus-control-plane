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

package com.netflix.titus.runtime.connector.eviction.replicator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.titus.api.eviction.model.EvictionQuota;
import com.netflix.titus.api.eviction.model.event.EvictionEvent;
import com.netflix.titus.api.eviction.model.event.EvictionQuotaEvent;
import com.netflix.titus.api.eviction.model.event.EvictionSnapshotEndEvent;
import com.netflix.titus.api.eviction.service.ReadOnlyEvictionOperations;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.api.model.reference.TierReference;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.runtime.connector.common.replicator.AbstractReplicatorEventStream;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicatorMetrics;
import com.netflix.titus.runtime.connector.common.replicator.ReplicatorEvent;
import com.netflix.titus.runtime.connector.eviction.EvictionDataSnapshot;
import com.netflix.titus.runtime.connector.eviction.EvictionServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

import static com.google.common.base.Preconditions.checkNotNull;

public class GrpcEvictionReplicatorEventStream extends AbstractReplicatorEventStream<EvictionDataSnapshot, EvictionEvent> {

    private static final Logger logger = LoggerFactory.getLogger(GrpcEvictionReplicatorEventStream.class);

    private final EvictionServiceClient client;

    public GrpcEvictionReplicatorEventStream(EvictionServiceClient client,
                                             DataReplicatorMetrics metrics,
                                             TitusRuntime titusRuntime,
                                             Scheduler scheduler) {
        super(metrics, titusRuntime, scheduler);
        this.client = client;
    }

    @Override
    protected Flux<ReplicatorEvent<EvictionDataSnapshot, EvictionEvent>> newConnection() {
        return Flux.defer(() -> {
            CacheUpdater cacheUpdater = new CacheUpdater();
            logger.info("Connecting to the eviction event stream...");
            return client.observeEvents(true).flatMap(cacheUpdater::onEvent);
        });
    }

    private class CacheUpdater {

        private final List<EvictionEvent> snapshotEvents = new ArrayList<>();
        private final AtomicReference<EvictionDataSnapshot> lastSnapshotRef = new AtomicReference<>();

        private Flux<ReplicatorEvent<EvictionDataSnapshot, EvictionEvent>> onEvent(EvictionEvent event) {
            try {
                if (lastSnapshotRef.get() != null) {
                    return processSnapshotUpdate(event);
                }
                if (event instanceof EvictionSnapshotEndEvent) {
                    return buildInitialCache();
                }
                snapshotEvents.add(event);
            } catch (Exception e) {
                logger.warn("Unexpected error when handling the agent change notification: {}", event, e);
                return Flux.error(e); // Return error to force the cache reconnect.
            }
            return Flux.empty();
        }

        private Flux<ReplicatorEvent<EvictionDataSnapshot, EvictionEvent>> buildInitialCache() {
            EvictionQuota systemEvictionQuota = null;
            Map<Tier, EvictionQuota> tierEvictionQuotas = new HashMap<>();
            Map<String, EvictionQuota> capacityGroupEvictionQuotas = new HashMap<>();
            Map<String, EvictionQuota> jobEvictionQuotas = new HashMap<>();

            for (EvictionEvent event : snapshotEvents) {
                if (event instanceof EvictionQuotaEvent) {
                    EvictionQuota quota = ((EvictionQuotaEvent) event).getQuota();
                    switch (quota.getReference().getLevel()) {
                        case System:
                            systemEvictionQuota = quota;
                            break;
                        case Tier:
                            tierEvictionQuotas.put(((TierReference) quota.getReference()).getTier(), quota);
                            break;
                        case CapacityGroup:
                            capacityGroupEvictionQuotas.put(quota.getReference().getName(), quota);
                            break;
                        case Job:
                            jobEvictionQuotas.put(quota.getReference().getName(), quota);
                            break;
                    }
                }
            }

            // Clear so the garbage collector can reclaim the memory (we no longer need this data).
            snapshotEvents.clear();

            checkNotNull(systemEvictionQuota, "System eviction quota missing");
            tierEvictionQuotas.computeIfAbsent(Tier.Flex, tier -> EvictionQuota.tierQuota(tier, ReadOnlyEvictionOperations.VERY_HIGH_QUOTA, "Not supported yet"));
            tierEvictionQuotas.computeIfAbsent(Tier.Critical, tier -> EvictionQuota.tierQuota(tier, ReadOnlyEvictionOperations.VERY_HIGH_QUOTA, "Not supported yet"));

            EvictionDataSnapshot initialSnapshot = new EvictionDataSnapshot(
                    UUID.randomUUID().toString(),
                    systemEvictionQuota,
                    tierEvictionQuotas,
                    capacityGroupEvictionQuotas,
                    jobEvictionQuotas
            );

            lastSnapshotRef.set(initialSnapshot);
            return Flux.just(new ReplicatorEvent<>(initialSnapshot, EvictionSnapshotEndEvent.getInstance(), titusRuntime.getClock().wallTime()));
        }

        private Flux<ReplicatorEvent<EvictionDataSnapshot, EvictionEvent>> processSnapshotUpdate(EvictionEvent event) {
            EvictionDataSnapshot snapshot = lastSnapshotRef.get();
            Optional<EvictionDataSnapshot> newSnapshot = Optional.empty();

            if (event instanceof EvictionQuotaEvent) {
                newSnapshot = snapshot.updateEvictionQuota(((EvictionQuotaEvent) event).getQuota());
            } // Ignore all other events, as they are not relevant for snapshot

            if (newSnapshot.isPresent()) {
                lastSnapshotRef.set(newSnapshot.get());
                return Flux.just(new ReplicatorEvent<>(newSnapshot.get(), event, titusRuntime.getClock().wallTime()));
            }
            return Flux.empty();
        }
    }
}
