package io.netflix.titus.common.util;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import io.netflix.titus.common.util.time.Clock;
import io.netflix.titus.common.util.time.Clocks;
import io.netflix.titus.common.util.tuple.Pair;

/**
 * Based on the provided configuration generates error occurrences in a stream of actions.
 */
public class ErrorGenerator {

    private final Supplier<Pair<Supplier<Boolean>, Long>> nextCycleFactory;
    private final Clock clock;

    private volatile Supplier<Boolean> failureIndicator;
    private volatile long endOfCycleTimestamp;

    private final Object lock = new Object();

    private ErrorGenerator(Supplier<Pair<Supplier<Boolean>, Long>> nextCycleFactory, Clock clock) {
        this.nextCycleFactory = nextCycleFactory;
        this.clock = clock;
    }

    /**
     * Returns true if the next operation should fail, and true if not.
     */
    public boolean shouldFail() {
        if (endOfCycleTimestamp >= 0 && endOfCycleTimestamp <= clock.wallTime()) {
            nextCycle();
        }
        return failureIndicator.get();
    }

    private void nextCycle() {
        synchronized (lock) {
            Pair<Supplier<Boolean>, Long> next = nextCycleFactory.get();

            long nexWindowMs = next.getRight();
            Supplier<Boolean> newFailureIndicator = next.getLeft();

            this.endOfCycleTimestamp = nexWindowMs < 0 ? -1 : clock.wallTime() + nexWindowMs;
            this.failureIndicator = newFailureIndicator;
        }
    }

    public static ErrorGenerator never() {
        return new ErrorGenerator(() -> Pair.of(() -> false, Long.MAX_VALUE / 2), Clocks.system());
    }

    public static ErrorGenerator always() {
        return new ErrorGenerator(() -> Pair.of(() -> true, Long.MAX_VALUE / 2), Clocks.system());
    }

    public static ErrorGenerator failuresWithFixedRate(double ratio, Clock clock) {
        return new ErrorGenerator(() -> failuresWithFixedRateInternal(ratio, -1), clock);
    }

    public static ErrorGenerator periodicOutages(long upTimeMs, long downTimeMs, Clock clock) {
        return new ErrorGenerator(alternating(alwaysUpFactory(upTimeMs), alwaysDownFactory(downTimeMs)), clock);
    }

    public static ErrorGenerator periodicFailures(long upTimeMs, long errorTimeMs, double ratio, Clock clock) {
        Preconditions.checkArgument(upTimeMs > 0 || errorTimeMs > 0, "Both upTime and errorTime cannot be equal to 0");

        if (upTimeMs <= 0) {
            return failuresWithFixedRate(ratio, clock);
        }
        if (errorTimeMs <= 0) {
            return never();
        }
        return new ErrorGenerator(alternating(alwaysUpFactory(upTimeMs), failuresWithFixedRateInternal(ratio, errorTimeMs)), clock);
    }

    private static Pair<Supplier<Boolean>, Long> failuresWithFixedRateInternal(double ratio, long downTimeMs) {
        AtomicLong total = new AtomicLong(1);
        AtomicLong failures = new AtomicLong(0);
        Supplier<Boolean> errorFunction = () -> {
            double currentRatio = ((double) failures.get()) / total.incrementAndGet();
            if (currentRatio < ratio) {
                failures.incrementAndGet();
                return true;
            }
            return false;
        };
        return Pair.of(errorFunction, downTimeMs);
    }

    private static Pair<Supplier<Boolean>, Long> alwaysUpFactory(long upTimeMs) {
        return Pair.of(() -> false, upTimeMs);
    }

    private static Pair<Supplier<Boolean>, Long> alwaysDownFactory(long downTimeMs) {
        return Pair.of(() -> true, downTimeMs);
    }

    private static Supplier<Pair<Supplier<Boolean>, Long>> alternating(Pair<Supplier<Boolean>, Long>... sources) {
        AtomicLong index = new AtomicLong(0);
        return () -> {
            int current = (int) (index.getAndIncrement() % sources.length);
            return sources[current];
        };
    }
}
