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

import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.limiter.tokenbucket.RefillStrategy;

/**
 * Adds Spectator metrics to a {@link RefillStrategy} instance.
 */
public class SpectatorRefillStrategyDecorator implements RefillStrategy {

    private static final String NAME_PREFIX = "titus.common.tokenBucket.refillStrategy.";

    private final RefillStrategy delegate;

    private final Counter refillCounter;
    private final Id timeUntilNextRefillId;

    private final Registry registry;

    public SpectatorRefillStrategyDecorator(String bucketName,
                                            RefillStrategy delegate,
                                            TitusRuntime titusRuntime) {
        this.delegate = delegate;

        this.registry = titusRuntime.getRegistry();
        this.refillCounter = registry.counter(
                NAME_PREFIX + "refillCount",
                "bucketName", bucketName
        );
        this.timeUntilNextRefillId = registry.createId(
                NAME_PREFIX + "timeUntilNextRefill",
                "bucketName", bucketName
        );
        PolledMeter.using(registry)
                .withId(timeUntilNextRefillId)
                .monitorValue(this, self -> self.getTimeUntilNextRefill(TimeUnit.MILLISECONDS));
    }

    public void shutdown() {
        PolledMeter.remove(registry, timeUntilNextRefillId);
    }

    @Override
    public long refill() {
        long refill = delegate.refill();
        refillCounter.increment(refill);
        return refill;
    }

    @Override
    public long getTimeUntilNextRefill(TimeUnit unit) {
        return delegate.getTimeUntilNextRefill(unit);
    }

    @Override
    public String toString() {
        return delegate.toString();
    }
}
