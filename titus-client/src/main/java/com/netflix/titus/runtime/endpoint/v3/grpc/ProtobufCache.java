/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.runtime.endpoint.v3.grpc;

import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.common.framework.scheduler.ScheduleReference;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.time.Clock;

class ProtobufCache<E, C> {

    private static final String METRICS_ROOT = "protobufCache.";

    private final Function<E, C> converter;
    private final Supplier<Predicate<E>> expiredPredicateSupplier;
    private final GrpcObjectsCacheConfiguration configuration;
    private final Clock clock;

    @VisibleForTesting
    final ConcurrentMap<String, CacheEntry<E, C>> cache = new ConcurrentHashMap<>();

    @VisibleForTesting
    final ConcurrentMap<String, Long> toBeRemoved = new ConcurrentHashMap<>();

    private final ScheduleReference scheduleRef;

    private final Counter cacheHit;
    private final Counter cacheMiss;

    ProtobufCache(String name,
                  Function<E, C> converter,
                  Supplier<Predicate<E>> expiredPredicateSupplier,
                  GrpcObjectsCacheConfiguration configuration,
                  TitusRuntime titusRuntime) {
        this.converter = converter;
        this.expiredPredicateSupplier = expiredPredicateSupplier;
        this.configuration = configuration;
        this.clock = titusRuntime.getClock();

        Registry registry = titusRuntime.getRegistry();
        PolledMeter.using(registry).withId(registry.createId(METRICS_ROOT + "cacheSize", "cache", name)).monitorSize(cache);
        PolledMeter.using(registry).withId(registry.createId(METRICS_ROOT + "toBeRemoved", "cache", name)).monitorSize(toBeRemoved);
        this.cacheHit = registry.counter(METRICS_ROOT + "cacheAccess", "cache", name, "cacheHit", "true");
        this.cacheMiss = registry.counter(METRICS_ROOT + "cacheAccess", "cache", name, "cacheHit", "false");

        ScheduleDescriptor gcScheduleDescriptor = ScheduleDescriptor.newBuilder()
                .withName(ProtobufCache.class.getSimpleName() + "#" + name)
                .withDescription("Protobuf entity cache")
                .withInitialDelay(Duration.ofMillis(configuration.getCacheRefreshInitialDelayMs()))
                .withInterval(Duration.ofMillis(configuration.getCacheRefreshIntervalMs()))
                .withTimeout(Duration.ofMillis(configuration.getCacheRefreshTimeoutMs()))
                .build();
        this.scheduleRef = titusRuntime.getLocalScheduler().schedule(gcScheduleDescriptor, context -> removeExpiredEntries(), true);
    }

    void shutdown() {
        scheduleRef.cancel();
    }

    C get(String id, E entity) {
        CacheEntry<E, C> entry = cache.get(id);
        if (entry != null) {
            if (entry.getCoreEntity() == entity) {
                cacheHit.increment();
                return entry.getProtobufEntity();
            }
        }
        CacheEntry<E, C> newEntry = new CacheEntry<>(id, entity, converter.apply(entity));
        cache.put(id, newEntry);
        cacheMiss.increment();
        return newEntry.getProtobufEntity();
    }

    private void removeExpiredEntries() {
        long now = clock.wallTime();

        // Do cleanup
        Iterator<Map.Entry<String, Long>> it = toBeRemoved.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Long> entry = it.next();
            if (entry.getValue() < now) {
                it.remove();
                cache.remove(entry.getKey());
            }
        }

        // Find expired entries
        Predicate<E> expiredPredicate = expiredPredicateSupplier.get();
        Set<String> candidates = CollectionsExt.copyAndRemove(cache.keySet(), toBeRemoved.keySet());
        candidates.forEach(id -> {
            CacheEntry<E, C> entry = cache.get(id);
            if (expiredPredicate.test(entry.getCoreEntity())) {
                toBeRemoved.put(id, now + configuration.getCacheCleanupDelayMs());
            }
        });
    }

    private static class CacheEntry<C, G> {

        private final String id;
        private final C coreEntity;
        private final G protobufEntity;

        private CacheEntry(String id, C coreEntity, G protobufEntity) {
            this.id = id;
            this.coreEntity = coreEntity;
            this.protobufEntity = protobufEntity;
        }

        public String getId() {
            return id;
        }

        public C getCoreEntity() {
            return coreEntity;
        }

        public G getProtobufEntity() {
            return protobufEntity;
        }
    }
}
