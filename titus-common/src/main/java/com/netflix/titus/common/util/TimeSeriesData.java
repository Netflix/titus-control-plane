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
 * Simple, concurrent time series data collector.
 */
public class TimeSeriesData {

    private static final Pair<Double, Long> CLEANUP_MARKER = Pair.of(0.0, 0L);

    private final long retentionMs;
    private final long stepMs;

    private final BiFunction<Double, Long, Double> adjuster;
    private final Clock clock;

    private final Queue<Pair<Double, Long>> offers = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedDeque<Pair<Double, Long>> data = new ConcurrentLinkedDeque<>();
    private final AtomicReference<Pair<Double, Long>> lastAggregate;

    private final Pair<Double, Long> nothing;
    private final AtomicInteger wipMarker = new AtomicInteger();

    public TimeSeriesData(long retentionMs,
                          long stepMs,
                          BiFunction<Double, Long, Double> adjuster,
                          Clock clock) {
        this.retentionMs = retentionMs;
        this.stepMs = stepMs;
        this.adjuster = adjuster;
        this.clock = clock;

        this.nothing = Pair.of(0.0, 0L);
        this.lastAggregate = new AtomicReference<>(nothing);
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
            process(now);
        }
        return lastAggregate.get().getLeft();
    }

    private void process(long now) {
        if (wipMarker.getAndIncrement() == 0) {
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
        }
    }

    private Pair<Double, Long> computeAggregatedValue(long now) {
        if (data.isEmpty()) {
            return nothing;
        }

        double sum = 0;
        for (Pair<Double, Long> next : data) {
            sum += adjuster.apply(next.getLeft(), now - next.getRight());
        }

        return Pair.of(sum, now);
    }
}
