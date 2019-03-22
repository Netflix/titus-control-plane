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

package com.netflix.titus.runtime.connector.common.replicator;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.time.Clock;

/**
 * Recommended metrics that should be reported by {@link DataReplicator} implementations.
 */
public class DataReplicatorMetrics<SNAPSHOT, TRIGGER> {

    private static final String ROOT = "titus.dataReplicator.";

    private final String source;
    private final Clock clock;
    private final Registry registry;

    private final AtomicLong connected = new AtomicLong();
    private final Id failuresId;
    private final Gauge staleness;
    private final ConcurrentMap<String, AtomicLong> cacheCollectionSizes = new ConcurrentHashMap<>();

    public DataReplicatorMetrics(String source, TitusRuntime titusRuntime) {
        this.source = source;
        this.clock = titusRuntime.getClock();
        this.registry = titusRuntime.getRegistry();

        // Use PolledMeter as this metric is set infrequently
        PolledMeter.using(registry).withId(registry.createId(ROOT + "connected", "source", source)).monitorValue(connected);

        this.failuresId = registry.createId(ROOT + "failures", "source", source);
        this.staleness = registry.gauge(ROOT + "staleness", "source", source);
    }

    public void connected() {
        connected.set(1);
    }

    public void disconnected() {
        connected.set(0);
        staleness.set(0);
    }

    public void disconnected(Throwable error) {
        disconnected();
        registry.counter(failuresId.withTags("error", error.getClass().getSimpleName())).increment();
    }

    public void event(ReplicatorEvent<SNAPSHOT, TRIGGER> event) {
        staleness.set(clock.wallTime() - event.getLastUpdateTime());
    }

    protected void setCacheCollectionSize(String name, long size) {
        cacheCollectionSizes.computeIfAbsent(name, n -> PolledMeter.using(registry)
                .withId(registry.createId(ROOT + "cache", "source", source, "cacheCollection", name))
                .monitorValue(new AtomicLong())
        ).set(size);
    }
}
