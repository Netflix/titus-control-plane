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

import io.netflix.titus.common.util.limiter.tokenbucket.RefillStrategy;
import io.netflix.titus.common.util.limiter.tokenbucket.TokenBucket;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DefaultTokenBucketTest {

    @Test
    public void tryTakeShouldGetToken() {
        TestRefillStrategy testRefillStrategy = createTestRefillStrategy();
        TokenBucket tokenBucket = createTokenBucket(10, testRefillStrategy, 0);
        testRefillStrategy.setAmountToRefill(1);
        boolean gotToken = tokenBucket.tryTake();
        assertTrue(gotToken);

        testRefillStrategy.setAmountToRefill(10);
        gotToken = tokenBucket.tryTake(10);
        assertTrue(gotToken);
    }

    @Test
    public void tryTakeShouldNotGetToken() {
        TestRefillStrategy testRefillStrategy = createTestRefillStrategy();
        TokenBucket tokenBucket = createTokenBucket(10, testRefillStrategy, 0);
        boolean gotToken = tokenBucket.tryTake();
        assertFalse(gotToken);

        testRefillStrategy.setAmountToRefill(5);
        gotToken = tokenBucket.tryTake(6);
        assertFalse(gotToken);
    }

    @Test
    public void takeShouldGetToken() {
        TestRefillStrategy testRefillStrategy = createTestRefillStrategy();
        TokenBucket tokenBucket = createTokenBucket(10, testRefillStrategy, 0);
        testRefillStrategy.setAmountToRefill(1);
        tokenBucket.take();

        testRefillStrategy.setAmountToRefill(10);
        tokenBucket.take(10);
    }

    private static class TestRefillStrategy implements RefillStrategy {

        private final Object mutex = new Object();

        private volatile long amountToRefill;
        private volatile long timeUntilNextRefill;

        @Override
        public long refill() {
            return amountToRefill;
        }

        @Override
        public long getTimeUntilNextRefill(TimeUnit unit) {
            return unit.convert(timeUntilNextRefill, TimeUnit.NANOSECONDS);
        }

        public void setAmountToRefill(long amountToRefill) {
            synchronized (mutex) {
                this.amountToRefill = amountToRefill;
            }
        }

        public void setTimeUntilNextRefill(long timeUntilNextRefill, TimeUnit unit) {
            synchronized (mutex) {
                this.timeUntilNextRefill = unit.toNanos(timeUntilNextRefill);
            }
        }
    }


    private TestRefillStrategy createTestRefillStrategy() {
        return new TestRefillStrategy();
    }

    private TokenBucket createTokenBucket(long capacity, RefillStrategy refillStrategy, long initialNumberOfTokens) {
        return new DefaultTokenBucket("TestTokenBucket", capacity, refillStrategy, initialNumberOfTokens);
    }
}