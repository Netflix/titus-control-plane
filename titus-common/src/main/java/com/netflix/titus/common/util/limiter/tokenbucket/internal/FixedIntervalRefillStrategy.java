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
import com.netflix.titus.common.util.limiter.tokenbucket.RefillStrategy;

public class FixedIntervalRefillStrategy implements RefillStrategy {
    private final Object mutex = new Object();

    private final Stopwatch stopwatch;
    private final long numberOfTokensPerInterval;
    private final long intervalInNanos;
    private long lastRefillTime;
    private long nextRefillTime;

    public FixedIntervalRefillStrategy(Stopwatch stopwatch, long numberOfTokensPerInterval, long interval, TimeUnit unit) {
        this.stopwatch = stopwatch;
        this.numberOfTokensPerInterval = numberOfTokensPerInterval;
        this.intervalInNanos = unit.toNanos(interval);

        this.lastRefillTime = -intervalInNanos;
        this.nextRefillTime = -intervalInNanos;

        if (!this.stopwatch.isRunning()) {
            this.stopwatch.start();
        }
    }

    @Override
    public long refill() {
        synchronized (mutex) {
            long elapsed = stopwatch.elapsed(TimeUnit.NANOSECONDS);
            if (elapsed < nextRefillTime) {
                return 0;
            }
            long numberOfIntervals = Math.max(0, (elapsed - lastRefillTime) / intervalInNanos);
            lastRefillTime += numberOfIntervals * intervalInNanos;
            nextRefillTime = lastRefillTime + intervalInNanos;

            return numberOfIntervals * numberOfTokensPerInterval;
        }
    }

    @Override
    public long getTimeUntilNextRefill(TimeUnit unit) {
        long elapsed = stopwatch.elapsed(TimeUnit.NANOSECONDS);
        return unit.convert(Math.max(0, nextRefillTime - elapsed), TimeUnit.NANOSECONDS);
    }
}
