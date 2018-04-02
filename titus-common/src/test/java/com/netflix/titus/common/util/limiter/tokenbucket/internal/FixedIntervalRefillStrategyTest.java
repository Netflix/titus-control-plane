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

package com.netflix.titus.common.util.limiter.tokenbucket.internal;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;
import com.netflix.titus.common.util.limiter.tokenbucket.RefillStrategy;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FixedIntervalRefillStrategyTest {

    @Test
    public void refillShouldReturn10() {
        TestTicker testTicker = createTestTicker();
        Stopwatch stopwatch = Stopwatch.createStarted(testTicker);
        testTicker.setNanos(TimeUnit.SECONDS.toNanos(9));

        RefillStrategy refillStrategy = new FixedIntervalRefillStrategy(stopwatch, 1, 1, TimeUnit.SECONDS);

        assertEquals(10, refillStrategy.refill());
    }

    @Test
    public void timeUntilNextRefillShouldReturn1000() {
        TestTicker testTicker = createTestTicker();
        Stopwatch stopwatch = Stopwatch.createStarted(testTicker);

        RefillStrategy refillStrategy = new FixedIntervalRefillStrategy(stopwatch, 1, 5, TimeUnit.SECONDS);
        refillStrategy.refill();

        long timeUntilNextRefill = refillStrategy.getTimeUntilNextRefill(TimeUnit.MILLISECONDS);
        System.out.println(timeUntilNextRefill);
        assertEquals(5_000, timeUntilNextRefill);
    }

    private static class TestTicker extends Ticker {

        private final Object mutex = new Object();

        private volatile long nanos;

        public void setNanos(long nanos) {
            synchronized (mutex) {
                this.nanos = nanos;
            }
        }

        @Override
        public long read() {
            return nanos;
        }
    }

    private TestTicker createTestTicker() {
        return new TestTicker();
    }
}