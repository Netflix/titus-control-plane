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

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.base.Stopwatch;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.limiter.tokenbucket.FixedIntervalTokenBucketConfiguration;
import com.netflix.titus.common.util.limiter.tokenbucket.RefillStrategy;
import com.netflix.titus.common.util.limiter.tokenbucket.TokenBucket;
import com.netflix.titus.common.util.limiter.tokenbucket.internal.DefaultTokenBucket;
import com.netflix.titus.common.util.limiter.tokenbucket.internal.DynamicTokenBucketDelegate;
import com.netflix.titus.common.util.limiter.tokenbucket.internal.FixedIntervalRefillStrategy;
import com.netflix.titus.common.util.limiter.tokenbucket.internal.FixedIntervalTokenBucketSupplier;
import com.netflix.titus.common.util.limiter.tokenbucket.internal.SpectatorTokenBucketDecorator;

public class Limiters {

    private Limiters() {
    }

    /**
     * Useful for testing.
     */
    public static TokenBucket unlimited(String name) {
        return new Unlimited(name);
    }

    /**
     * Create a {@link TokenBucket} with a fixed interval {@link RefillStrategy}.
     */
    public static TokenBucket createFixedIntervalTokenBucket(String name, long capacity, long initialNumberOfTokens,
                                                             long numberOfTokensPerInterval, long interval, TimeUnit unit) {
        RefillStrategy refillStrategy = new FixedIntervalRefillStrategy(Stopwatch.createStarted(),
                numberOfTokensPerInterval, interval, unit);
        return new DefaultTokenBucket(name, capacity, refillStrategy, initialNumberOfTokens);
    }

    /**
     * Create a {@link TokenBucket} with a fixed interval {@link RefillStrategy}. The token bucket configuration is
     * checked on each invocation, and the bucket is automatically recreated if it changes.
     */
    public static TokenBucket createFixedIntervalTokenBucket(String name,
                                                             FixedIntervalTokenBucketConfiguration configuration,
                                                             Consumer<TokenBucket> onChangeListener) {
        return new DynamicTokenBucketDelegate(
                new FixedIntervalTokenBucketSupplier(name, configuration, onChangeListener, Optional.empty())
        );
    }

    /**
     * Functionally equivalent to {@link #createFixedIntervalTokenBucket(String, FixedIntervalTokenBucketConfiguration, Consumer)},
     * but with Spectator metrics.
     */
    public static TokenBucket createInstrumentedFixedIntervalTokenBucket(String name,
                                                                         FixedIntervalTokenBucketConfiguration configuration,
                                                                         Consumer<TokenBucket> onChangeListener,
                                                                         TitusRuntime titusRuntime) {
        return new SpectatorTokenBucketDecorator(
                new DynamicTokenBucketDelegate(
                        new FixedIntervalTokenBucketSupplier(name, configuration, onChangeListener, Optional.of(titusRuntime))
                ),
                titusRuntime
        );
    }

    private static class Unlimited implements TokenBucket {
        private final String name;
        private final UnlimitedRefillStrategy strategy = new UnlimitedRefillStrategy();

        public Unlimited(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public long getCapacity() {
            return Long.MAX_VALUE;
        }

        @Override
        public long getNumberOfTokens() {
            return Long.MAX_VALUE;
        }

        @Override
        public boolean tryTake() {
            return true;
        }

        @Override
        public boolean tryTake(long numberOfTokens) {
            return true;
        }

        @Override
        public void take() {
        }

        @Override
        public void take(long numberOfTokens) {
        }

        @Override
        public void refill(long numberOfToken) {
        }

        @Override
        public RefillStrategy getRefillStrategy() {
            return strategy;
        }

    }

    private static class UnlimitedRefillStrategy implements RefillStrategy {
        @Override
        public long refill() {
            return 0;
        }

        @Override
        public long getTimeUntilNextRefill(TimeUnit unit) {
            return 0;
        }
    }
}
