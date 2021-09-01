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

package com.netflix.titus.ext.jooq.spectator;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.spectator.MetricSelector;
import com.netflix.titus.common.util.spectator.SpectatorExt;
import com.netflix.titus.common.util.spectator.ValueRangeCounter;
import com.zaxxer.hikari.metrics.IMetricsTracker;
import com.zaxxer.hikari.metrics.MetricsTrackerFactory;
import com.zaxxer.hikari.metrics.PoolStats;

public class SpectatorHikariMetricsTrackerFactory implements MetricsTrackerFactory {

    private static final long ONE_MILLION = 1_000_000;

    private final Registry registry;

    private final Id idPoolStats;
    private final Id idLatencies;
    private final Id idConnectionTimeout;

    public SpectatorHikariMetricsTrackerFactory(Id root, TitusRuntime titusRuntime) {
        this.registry = titusRuntime.getRegistry();
        this.idPoolStats = registry.createId(root.name() + ".poolStats", root.tags());
        this.idLatencies = registry.createId(root.name() + ".latencies", root.tags());
        this.idConnectionTimeout = registry.createId(root.name() + ".connectionTimeout", root.tags());
    }

    @Override
    public IMetricsTracker create(String poolName, PoolStats poolStats) {
        return new SpectatorIMetricsTracker(poolName, poolStats);
    }

    private class SpectatorIMetricsTracker implements IMetricsTracker {

        private final MetricSelector<ValueRangeCounter> latencies;
        private final Counter connectionTimeoutCounter;

        // Keep reference to prevent it from being garbage collected.
        private final PoolStats poolStats;

        public SpectatorIMetricsTracker(String poolName, PoolStats poolStats) {
            this.poolStats = poolStats;
            PolledMeter.using(registry)
                    .withId(idPoolStats.withTags("pool", poolName, "poolStats", "totalConnections"))
                    .monitorValue(poolStats, PoolStats::getTotalConnections);
            PolledMeter.using(registry)
                    .withId(idPoolStats.withTags("pool", poolName, "poolStats", "activeConnections"))
                    .monitorValue(poolStats, PoolStats::getActiveConnections);
            PolledMeter.using(registry)
                    .withId(idPoolStats.withTags("pool", poolName, "poolStats", "idleConnections"))
                    .monitorValue(poolStats, PoolStats::getIdleConnections);
            PolledMeter.using(registry)
                    .withId(idPoolStats.withTags("pool", poolName, "poolStats", "maxConnections"))
                    .monitorValue(poolStats, PoolStats::getMaxConnections);
            PolledMeter.using(registry)
                    .withId(idPoolStats.withTags("pool", poolName, "poolStats", "minConnections"))
                    .monitorValue(poolStats, PoolStats::getMinConnections);
            PolledMeter.using(registry)
                    .withId(idPoolStats.withTags("pool", poolName, "poolStats", "pendingThreads"))
                    .monitorValue(poolStats, PoolStats::getPendingThreads);

            this.latencies = SpectatorExt.newValueRangeCounter(
                    idLatencies.withTags("pool", poolName),
                    new String[]{"step"},
                    SpectatorUtils.LEVELS,
                    registry
            );
            this.connectionTimeoutCounter = registry.counter(idConnectionTimeout.withTag("pool", poolName));
        }

        @Override
        public void recordConnectionCreatedMillis(long connectionCreatedMillis) {
            latencies.withTags("connectionCreated").ifPresent(m -> m.recordLevel(connectionCreatedMillis));
        }

        @Override
        public void recordConnectionAcquiredNanos(long elapsedAcquiredNanos) {
            latencies.withTags("connectionAcquired").ifPresent(m -> m.recordLevel(elapsedAcquiredNanos / ONE_MILLION));
        }

        @Override
        public void recordConnectionUsageMillis(long elapsedBorrowedMillis) {
            latencies.withTags("elapsedBorrowed").ifPresent(m -> m.recordLevel(elapsedBorrowedMillis));
        }

        @Override
        public void recordConnectionTimeout() {
            connectionTimeoutCounter.increment();
        }
    }
}
