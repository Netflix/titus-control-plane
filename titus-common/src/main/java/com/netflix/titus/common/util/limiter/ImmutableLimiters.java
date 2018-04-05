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

package com.netflix.titus.common.util.limiter;

import java.util.concurrent.TimeUnit;

import com.netflix.titus.common.util.limiter.tokenbucket.ImmutableTokenBucket;
import com.netflix.titus.common.util.limiter.tokenbucket.ImmutableTokenBucket.ImmutableRefillStrategy;
import com.netflix.titus.common.util.limiter.tokenbucket.internal.DefaultImmutableTokenBucket;
import com.netflix.titus.common.util.limiter.tokenbucket.internal.ImmutableFixedIntervalRefillStrategy;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.common.util.time.Clocks;

/**
 */
public final class ImmutableLimiters {
    private ImmutableLimiters() {
    }

    public static ImmutableRefillStrategy refillAtFixedInterval(long numberOfTokensPerInterval, long interval, TimeUnit timeUnit, Clock clock) {
        return new ImmutableFixedIntervalRefillStrategy(numberOfTokensPerInterval, timeUnit.toNanos(interval), clock);
    }

    public static ImmutableRefillStrategy refillAtFixedInterval(long numberOfTokensPerInterval, long interval, TimeUnit timeUnit) {
        return new ImmutableFixedIntervalRefillStrategy(numberOfTokensPerInterval, timeUnit.toNanos(interval), Clocks.system());
    }

    public static ImmutableTokenBucket tokenBucket(long bucketSize, ImmutableRefillStrategy refillStrategy) {
        return new DefaultImmutableTokenBucket(bucketSize, bucketSize, refillStrategy);
    }
}
