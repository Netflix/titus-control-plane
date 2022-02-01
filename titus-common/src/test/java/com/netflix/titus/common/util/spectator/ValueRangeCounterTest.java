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

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ValueRangeCounterTest {

    private static final long[] LEVELS = new long[]{1, 10, 50, 100, 200, 500};

    private final Registry registry = new DefaultRegistry();

    @Test
    public void testIncrement() {
        ValueRangeCounter counters = SpectatorExt.newValueRangeCounter(registry.createId("junit"), LEVELS, registry);

        counters.recordLevel(-1);
        assertThat(counters.counters.get(1L).count()).isEqualTo(1L);

        counters.recordLevel(49);
        assertThat(counters.counters.get(50L).count()).isEqualTo(1L);

        counters.recordLevel(499);
        assertThat(counters.counters.get(500L).count()).isEqualTo(1L);

        counters.recordLevel(500);
        counters.recordLevel(5000);
        assertThat(counters.unbounded.count()).isEqualTo(2);
    }

    @Test
    public void testNewSortableFormatter() {
        testNewSortableFormatter(1, "001", 1, 10, 100);
        testNewSortableFormatter(9, "009", 1, 10, 100);
        testNewSortableFormatter(10, "010", 1, 10, 100);
        testNewSortableFormatter(99, "099", 1, 10, 100);
        testNewSortableFormatter(100, "100", 1, 10, 100);

        testNewSortableFormatter(1, "001", 9, 99, 999);
        testNewSortableFormatter(999, "999", 9, 99, 999);
    }

    private void testNewSortableFormatter(long input, String expectedFormat, long... levels) {
        String actualFormat = ValueRangeCounter.newSortableFormatter(levels).apply(input);
        assertThat(actualFormat).isEqualTo(expectedFormat);
    }
}