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

import com.netflix.titus.common.util.DateTimeExt;
import com.netflix.titus.common.util.limiter.tokenbucket.RefillStrategy;
import com.netflix.titus.common.util.time.Clock;

public class FixedIntervalRefillStrategy implements RefillStrategy {

    private final Object mutex = new Object();

    private final long numberOfTokensPerInterval;
    private final long intervalInNanos;
    private final String toStringValue;
    private long lastRefillTimeNano;

    /**
     * Make it volatile, so we do not have to sync for reading.
     */
    private volatile long nextRefillTimeNano;

    private final Clock clock;

    public FixedIntervalRefillStrategy(long numberOfTokensPerInterval, long interval, TimeUnit unit, Clock clock) {
        this.numberOfTokensPerInterval = numberOfTokensPerInterval;
        this.clock = clock;
        this.intervalInNanos = unit.toNanos(interval);
        this.toStringValue = "FixedIntervalRefillStrategy{refillRate=" + DateTimeExt.toRateString(interval, numberOfTokensPerInterval, unit, "refill") + '}';

        long nowNano = clock.nanoTime();
        this.lastRefillTimeNano = nowNano - intervalInNanos;
        this.nextRefillTimeNano = nowNano - intervalInNanos;
    }

    @Override
    public long refill() {
        long nowNano = clock.nanoTime();
        if (nowNano < nextRefillTimeNano) {
            return 0;
        }
        synchronized (mutex) {
            if (nowNano < nextRefillTimeNano) {
                return 0;
            }
            long numberOfIntervals = Math.max(0, (nowNano - lastRefillTimeNano) / intervalInNanos);
            lastRefillTimeNano += numberOfIntervals * intervalInNanos;
            nextRefillTimeNano = lastRefillTimeNano + intervalInNanos;

            return numberOfIntervals * numberOfTokensPerInterval;
        }
    }

    @Override
    public long getTimeUntilNextRefill(TimeUnit unit) {
        long nowNano = clock.nanoTime();
        return unit.convert(Math.max(0, nextRefillTimeNano - nowNano), TimeUnit.NANOSECONDS);
    }

    @Override
    public String toString() {
        return toStringValue;
    }
}
