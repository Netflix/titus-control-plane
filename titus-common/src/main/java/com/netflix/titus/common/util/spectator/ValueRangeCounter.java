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

package com.netflix.titus.common.util.spectator;

import java.util.HashMap;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;

public class ValueRangeCounter {

    @VisibleForTesting
    final Map<Long, Counter> counters;

    private final Counter first;

    @VisibleForTesting
    final Counter unbounded;

    private final long[] levels;

    public ValueRangeCounter(Id rootId, long[] levels, Registry registry) {
        this.levels = levels;
        Map<Long, Counter> counters = new HashMap<>();
        for (long level : levels) {
            counters.put(level, registry.counter(rootId.withTag("level", Long.toString(level))));
        }
        this.counters = counters;
        this.first = counters.get(levels[0]);
        this.unbounded = registry.counter(rootId.withTag("level", "unbounded"));
    }

    public void recordLevel(long level) {
        if (level < levels[0]) {
            first.increment();
        } else if (level >= levels[levels.length - 1]) {
            unbounded.increment();
        } else {
            counters.get(toConfiguredLevel(level)).increment();
        }
    }

    private long toConfiguredLevel(long level) {
        for (long l : levels) {
            if (level < l) {
                return l;
            }
        }
        return levels[levels.length - 1];
    }
}
