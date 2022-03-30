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

import com.netflix.titus.common.util.limiter.tokenbucket.RefillStrategy;
import com.netflix.titus.common.util.time.Clocks;
import com.netflix.titus.common.util.time.TestClock;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FixedIntervalRefillStrategyTest {

    private final TestClock clock = Clocks.test();

    @Test
    public void refillShouldReturn10() {
        RefillStrategy refillStrategy = new FixedIntervalRefillStrategy(1, 1, TimeUnit.SECONDS, clock);
        clock.advanceTime(9, TimeUnit.SECONDS);
        assertEquals(10, refillStrategy.refill());
    }

    @Test
    public void timeUntilNextRefillShouldReturn1000() {
        RefillStrategy refillStrategy = new FixedIntervalRefillStrategy(1, 5, TimeUnit.SECONDS, clock);
        refillStrategy.refill();
        long timeUntilNextRefill = refillStrategy.getTimeUntilNextRefill(TimeUnit.MILLISECONDS);
        assertEquals(5_000, timeUntilNextRefill);
    }
}