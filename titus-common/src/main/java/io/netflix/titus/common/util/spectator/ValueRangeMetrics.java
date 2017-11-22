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

package io.netflix.titus.common.util.spectator;

import java.util.function.Function;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;

/**
 * Monitors a value, and a collection of buckets, each denoting a value range. If a current value is within a
 * particular bucket/range, its gauge is set to 1. All the other buckets are set to 0.
 * If a current value is less than zero, all gauge values are set to 0.
 */
public class ValueRangeMetrics<SOURCE> {

    public ValueRangeMetrics(Id rootId, long[] levels, SOURCE source, Function<SOURCE, Long> valueSupplier, Registry registry) {
        buildValueGauge(rootId, source, valueSupplier, registry);
        buildValueLevelGauges(rootId, levels, source, valueSupplier, registry);
    }

    private void buildValueGauge(Id rootId, SOURCE source, Function<SOURCE, Long> valueSupplier, Registry registry) {
        registry.gauge(
                registry.createId(rootId.name() + "current", rootId.tags()),
                source, sourceArg -> {
                    long newValue = valueSupplier.apply(sourceArg);
                    return newValue < 0 ? 0 : newValue;
                }
        );
    }

    private void buildValueLevelGauges(Id rootId, long[] levels, SOURCE source, Function<SOURCE, Long> valueSupplier, Registry registry) {
        Id levelsId = registry.createId(rootId.name() + "level", rootId.tags());
        for (int i = 0; i < levels.length; i++) {
            final long previous = i == 0 ? -1 : levels[i - 1];
            long level = levels[i];
            registry.gauge(levelsId.withTag("level", Long.toString(level)), source, sourceArg -> {
                long newValue = valueSupplier.apply(sourceArg);
                if (newValue < 0) {
                    return 0;
                }
                return newValue > previous && newValue <= level ? 1 : 0;
            });
        }
        final long lastLevel = levels[levels.length - 1];
        registry.gauge(levelsId.withTag("level", "unbounded"), source, sourceArg -> {
            long newValue = valueSupplier.apply(sourceArg);
            if (newValue < 0) {
                return 0;
            }
            return newValue > lastLevel ? 1 : 0;
        });
    }

    public static <SOURCE> ValueRangeMetrics<SOURCE> metricsOf(Id rootId,
                                                               long[] levels,
                                                               SOURCE source,
                                                               Function<SOURCE, Long> valueSupplier,
                                                               Registry registry) {
        return new ValueRangeMetrics<>(rootId, levels, source, valueSupplier, registry);
    }
}
