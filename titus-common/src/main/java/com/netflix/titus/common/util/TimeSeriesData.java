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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.common.util.tuple.Pair;

/**
 * Simple, concurrent time series data collector. Each data point consists of a value and a timestamp. The amount
 * of data to keep is controlled by a retention time. Data older than the retention time period are removed from
 * the data set. The class provides an aggregate of all currently held values. The raw values can be adjusted
 * by the user provided adjuster function. For example, the values can be weighted based on how old they are.
 */
public class TimeSeriesData {

    private static final Pair<Double, Long> CLEANUP_MARKER = Pair.of(-1.0, 0L);
    private static final Pair<Double, Long> NOTHING = Pair.of(0.0, 0L);

    private final long retentionMs;
    private final long stepMs;

    private final BiFunction<Double, Long, Double> adjuster;
    private final Clock clock;

    private final Queue<Pair<Double, Long>> offers = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedDeque<Pair<Double, Long>> data = new ConcurrentLinkedDeque<>();
    private final AtomicReference<Pair<Double, Long>> lastAggregate;

    private final AtomicInteger wipMarker = new AtomicInteger();

    /**
     * Constructor.
     *
     * @param retentionMs data retention time (data older than that are removed from the data set)
     * @param stepMs      computation accuracy with respect to time (important when the adjuster function uses elapsed time in the formula)
     * @param adjuster    a value adjuster function, taking as an argument the value itself, and the amount of time from its creation
     */
    public TimeSeriesData(long retentionMs,
                          long stepMs,
                          BiFunction<Double, Long, Double> adjuster,
                          Clock clock) {
        this.retentionMs = retentionMs;
        this.stepMs = stepMs;
        this.adjuster = adjuster;
        this.clock = clock;

        this.lastAggregate = new AtomicReference<>(NOTHING);
    }

    public void add(double value, long timestamp) {
        long now = clock.wallTime();
        if (now - retentionMs < timestamp) {
            offers.add(Pair.of(value, timestamp));
            process(now);
        }
    }

    public void clear() {
        if (!data.isEmpty()) {
            offers.add(CLEANUP_MARKER);
            process(clock.wallTime());
        }
    }

    public double getAggregatedValue() {
        if (data.isEmpty()) {
            return 0.0;
        }

        long now = clock.wallTime();
        if (now - lastAggregate.get().getRight() >= stepMs) {
            if (!process(now)) {
                return computeAggregatedValue(now).getLeft();
            }
        }
        return lastAggregate.get().getLeft();
    }

    private boolean process(long now) {
        if (wipMarker.getAndIncrement() != 0) {
            return false;
        }
        do {
            boolean changed = false;

            // Remove expired entries
            long expiredTimestamp = now - retentionMs;
            for (Pair<Double, Long> next; (next = data.peek()) != null && next.getRight() <= expiredTimestamp; ) {
                data.poll();
                changed = true;
            }

            // Add new entries
            Pair<Double, Long> offer;
            while ((offer = offers.poll()) != null) {
                long latest = data.isEmpty() ? 0 : data.getLast().getRight();
                if (offer == CLEANUP_MARKER) {
                    data.clear();
                    changed = true;
                } else if (offer.getRight() >= latest) {
                    data.add(offer);
                    changed = true;
                }
            }

            // Recompute the aggregated value if needed
            if (changed || now - lastAggregate.get().getRight() >= stepMs) {
                lastAggregate.set(computeAggregatedValue(now));
            }
        } while (wipMarker.decrementAndGet() != 0);

        return true;
    }

    private Pair<Double, Long> computeAggregatedValue(long now) {
        if (data.isEmpty()) {
            return NOTHING;
        }

        double sum = 0;
        for (Pair<Double, Long> next : data) {
            sum += adjuster.apply(next.getLeft(), now - next.getRight());
        }

        return Pair.of(sum, now);
    }
}
