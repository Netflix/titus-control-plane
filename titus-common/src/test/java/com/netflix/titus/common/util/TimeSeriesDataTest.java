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

package com.netflix.titus.common.util;

import java.util.concurrent.TimeUnit;

import com.netflix.titus.common.util.time.Clocks;
import com.netflix.titus.common.util.time.TestClock;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TimeSeriesDataTest {

    private static final long RETENTION_MS = 60_000;
    private static final long STEP_MS = 1_000;

    private final TestClock testClock = Clocks.test();

    private final TimeSeriesData timeSeriesData = new TimeSeriesData(
            RETENTION_MS,
            STEP_MS,
            (value, delayMs) -> value / (1 + delayMs / 1_000),
            testClock
    );

    @Test
    public void testAggregate() {
        timeSeriesData.add(30, testClock.wallTime());
        testClock.advanceTime(2_000, TimeUnit.MILLISECONDS);
        timeSeriesData.add(20, testClock.wallTime());
        assertThat(timeSeriesData.getAggregatedValue()).isEqualTo(30);
    }

    @Test
    public void testDataExpiry() {
        timeSeriesData.add(10, testClock.wallTime());
        testClock.advanceTime(60_000, TimeUnit.MILLISECONDS);
        timeSeriesData.add(10, testClock.wallTime());
        assertThat(timeSeriesData.getAggregatedValue()).isEqualTo(10);
    }

    @Test
    public void testClear() {
        timeSeriesData.add(10, testClock.wallTime());
        timeSeriesData.clear();
        assertThat(timeSeriesData.getAggregatedValue()).isEqualTo(0);
    }
}