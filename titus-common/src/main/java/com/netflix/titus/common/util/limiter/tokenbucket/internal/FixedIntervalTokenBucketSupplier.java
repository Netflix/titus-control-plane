/*
 * Copyright 2020 Netflix, Inc.
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

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.base.Stopwatch;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.common.util.limiter.tokenbucket.FixedIntervalTokenBucketConfiguration;
import com.netflix.titus.common.util.limiter.tokenbucket.RefillStrategy;
import com.netflix.titus.common.util.limiter.tokenbucket.TokenBucket;

/**
 * {@link TokenBucket} supplier which recreates a token bucket if any of its configurable parameters changes.
 * The configuration parameters are read from {@link FixedIntervalTokenBucketConfiguration}.
 */
public class FixedIntervalTokenBucketSupplier implements Supplier<TokenBucket> {

    private final String name;
    private final FixedIntervalTokenBucketConfiguration configuration;
    private final Consumer<TokenBucket> onChangeListener;
    private final Optional<TitusRuntime> titusRuntime;

    private volatile ActiveConfiguration activeConfiguration;

    private final Object lock = new Object();

    public FixedIntervalTokenBucketSupplier(String name,
                                            FixedIntervalTokenBucketConfiguration configuration,
                                            Consumer<TokenBucket> onChangeListener,
                                            Optional<TitusRuntime> titusRuntime) {
        this.name = name;
        this.configuration = configuration;
        this.onChangeListener = onChangeListener;
        this.titusRuntime = titusRuntime;
        this.activeConfiguration = reload();
    }

    @Override
    public TokenBucket get() {
        return isSame() ? activeConfiguration.getTokenBucket() : reload().getTokenBucket();
    }

    private boolean isSame() {
        return activeConfiguration != null
                && activeConfiguration.getCapacity() == configuration.getCapacity()
                && activeConfiguration.getInitialNumberOfTokens() == configuration.getInitialNumberOfTokens()
                && activeConfiguration.getIntervalMs() == configuration.getIntervalMs()
                && activeConfiguration.getNumberOfTokensPerInterval() == configuration.getNumberOfTokensPerInterval();
    }

    private ActiveConfiguration reload() {
        boolean same;
        synchronized (lock) {
            same = isSame();
            if (!same) {
                Evaluators.acceptNotNull(activeConfiguration, ActiveConfiguration::shutdown);
                this.activeConfiguration = new ActiveConfiguration(configuration);
            }
        }

        if (!same) {
            ExceptionExt.silent(() -> onChangeListener.accept(activeConfiguration.getTokenBucket()));
        }

        return this.activeConfiguration;
    }

    private class ActiveConfiguration {

        private final TokenBucket tokenBucket;
        private final long capacity;
        private final long initialNumberOfTokens;
        private final long intervalMs;
        private final long numberOfTokensPerInterval;

        private final RefillStrategy refillStrategy;

        private ActiveConfiguration(FixedIntervalTokenBucketConfiguration configuration) {
            this.capacity = configuration.getCapacity();
            this.initialNumberOfTokens = configuration.getInitialNumberOfTokens();
            this.intervalMs = configuration.getIntervalMs();
            this.numberOfTokensPerInterval = configuration.getNumberOfTokensPerInterval();

            RefillStrategy baseRefillStrategy = new FixedIntervalRefillStrategy(
                    Stopwatch.createStarted(),
                    numberOfTokensPerInterval,
                    intervalMs, TimeUnit.MILLISECONDS
            );

            this.refillStrategy = titusRuntime.map(runtime ->
                    (RefillStrategy) new SpectatorRefillStrategyDecorator(name, baseRefillStrategy, runtime))
                    .orElse(baseRefillStrategy);

            this.tokenBucket = new DefaultTokenBucket(
                    name,
                    capacity,
                    refillStrategy,
                    initialNumberOfTokens
            );
        }

        private void shutdown() {
            if (refillStrategy instanceof SpectatorRefillStrategyDecorator) {
                ((SpectatorRefillStrategyDecorator) refillStrategy).shutdown();
            }
        }

        private long getCapacity() {
            return capacity;
        }

        private long getInitialNumberOfTokens() {
            return initialNumberOfTokens;
        }

        private long getIntervalMs() {
            return intervalMs;
        }

        private long getNumberOfTokensPerInterval() {
            return numberOfTokensPerInterval;
        }

        private TokenBucket getTokenBucket() {
            return tokenBucket;
        }
    }
}
