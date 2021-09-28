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

package com.netflix.titus.common.util.retry;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.util.retry.internal.ExponentialBackoffRetryer;
import com.netflix.titus.common.util.retry.internal.ImmediateRetryer;
import com.netflix.titus.common.util.retry.internal.IntervalRetryer;
import com.netflix.titus.common.util.retry.internal.MaxManyRetryers;
import com.netflix.titus.common.util.retry.internal.NeverRetryer;

/**
 *
 */
public final class Retryers {

    private Retryers() {
    }

    public static Retryer never() {
        return NeverRetryer.INSTANCE;
    }

    public static Retryer immediate() {
        return ImmediateRetryer.UNLIMITED;
    }

    public static Retryer immediate(int limit) {
        Preconditions.checkArgument(limit > 0, "Retry limit (%s) must be > 0", limit);
        return new ImmediateRetryer(limit);
    }

    public static Retryer interval(long delay, TimeUnit timeUnit) {
        return interval(delay, timeUnit, Integer.MAX_VALUE);
    }

    public static Retryer interval(long delay, TimeUnit timeUnit, int limit) {
        Preconditions.checkArgument(delay >= 0, "Delay cannot be negative: %s", delay);
        Preconditions.checkArgument(limit > 0, "Retry limit (%s) must be > 0", limit);
        return new IntervalRetryer(Optional.of(timeUnit.toMillis(delay)), limit);
    }

    public static Retryer exponentialBackoff(long initialDelay, long maxDelay, TimeUnit timeUnit) {
        return exponentialBackoff(initialDelay, maxDelay, timeUnit, Integer.MAX_VALUE);
    }

    public static Retryer exponentialBackoff(long initialDelay, long maxDelay, TimeUnit timeUnit, int limit) {
        Preconditions.checkArgument(initialDelay >= 0, "Initial delay cannot be negative: %s", initialDelay);
        Preconditions.checkArgument(maxDelay >= initialDelay, "Max delay (%s) must be >= initial delay (%s)", maxDelay, initialDelay);
        Preconditions.checkArgument(limit > 0, "Retry limit (%s) must be > 0", limit);

        return new ExponentialBackoffRetryer(
                Optional.of(timeUnit.toMillis(initialDelay)),
                timeUnit.toMillis(maxDelay),
                limit
        );
    }

    /**
     * For each execution evaluates all retryers and returns the maximum delay.
     */
    public static Retryer max(Retryer... retryers) {
        Preconditions.checkArgument(retryers.length > 0, "At least one retryer expected");
        if (retryers.length == 1) {
            return retryers[0];
        }
        return new MaxManyRetryers(retryers);
    }
}
