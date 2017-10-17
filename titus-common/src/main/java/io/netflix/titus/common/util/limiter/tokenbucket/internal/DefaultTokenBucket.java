/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.common.util.limiter.tokenbucket.internal;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.Uninterruptibles;
import io.netflix.titus.common.util.limiter.tokenbucket.RefillStrategy;
import io.netflix.titus.common.util.limiter.tokenbucket.TokenBucket;

public class DefaultTokenBucket implements TokenBucket {

    private final Object mutex = new Object();

    private final String name;
    private final long capacity;
    private final RefillStrategy refillStrategy;
    private volatile long numberOfTokens;

    public DefaultTokenBucket(String name, long capacity, RefillStrategy refillStrategy, long initialNumberOfTokens) {

        Preconditions.checkArgument(!Strings.isNullOrEmpty(name), "Name must not be null or empty.");
        Preconditions.checkArgument(capacity > 0, "Capacity must be greater than 0.");
        Preconditions.checkNotNull(refillStrategy);
        Preconditions.checkArgument(initialNumberOfTokens >= 0, "Capacity must not be negative.");

        this.name = name;
        this.capacity = capacity;
        this.refillStrategy = refillStrategy;
        this.numberOfTokens = initialNumberOfTokens;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public long getCapacity() {
        return capacity;
    }

    @Override
    public long getNumberOfTokens() {
        refill(refillStrategy.refill());
        return numberOfTokens;
    }

    @Override
    public boolean tryTake() {
        return tryTake(1);
    }

    @Override
    public boolean tryTake(long numberOfTokens) {

        Preconditions.checkArgument(numberOfTokens > 0, "Number of tokens must be greater than 0.");
        Preconditions.checkArgument(numberOfTokens <= capacity, "Number of tokens must not be greater than the capacity.");

        synchronized (mutex) {
            refill(refillStrategy.refill());

            if (numberOfTokens <= this.numberOfTokens) {
                this.numberOfTokens -= numberOfTokens;
                return true;
            }

            return false;
        }
    }

    @Override
    public void take() {
        take(1);
    }

    @Override
    public void take(long numberOfTokens) {

        Preconditions.checkArgument(numberOfTokens > 0, "Number of tokens must be greater than 0.");
        Preconditions.checkArgument(numberOfTokens <= capacity, "Number of tokens must not be greater than the capacity.");

        while (true) {
            if (tryTake(numberOfTokens)) {
                break;
            }
            long timeUntilNextRefill = refillStrategy.getTimeUntilNextRefill(TimeUnit.NANOSECONDS);
            if (timeUntilNextRefill > 0) {
                Uninterruptibles.sleepUninterruptibly(timeUntilNextRefill, TimeUnit.NANOSECONDS);
            }
        }
    }

    @Override
    public void refill(long numberOfTokens) {
        synchronized (mutex) {
            this.numberOfTokens = Math.min(capacity, Math.max(0, this.numberOfTokens + numberOfTokens));
        }
    }

    @Override
    public RefillStrategy getRefillStrategy() {
        return refillStrategy;
    }
}
