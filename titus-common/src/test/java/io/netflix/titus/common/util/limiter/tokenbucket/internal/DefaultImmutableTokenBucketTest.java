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

import io.netflix.titus.common.util.limiter.tokenbucket.ImmutableTokenBucket;
import io.netflix.titus.common.util.time.Clocks;
import io.netflix.titus.common.util.time.TestClock;
import io.netflix.titus.common.util.tuple.Pair;
import org.junit.Test;

import static io.netflix.titus.common.util.limiter.ImmutableLimiters.refillAtFixedInterval;
import static io.netflix.titus.common.util.limiter.ImmutableLimiters.tokenBucket;
import static org.assertj.core.api.Assertions.assertThat;

/**
 */
public class DefaultImmutableTokenBucketTest {

    private static final int BUCKET_SIZE = 5;
    private static final int REFILL_INTERVAL_MS = 100;

    private final TestClock testClock = Clocks.test();

    private final ImmutableTokenBucket tokenBucket = tokenBucket(
            BUCKET_SIZE,
            refillAtFixedInterval(1, REFILL_INTERVAL_MS, TimeUnit.MILLISECONDS, testClock)
    );

    @Test
    public void testTryTake() throws Exception {
        ImmutableTokenBucket next = tokenBucket;
        for (int i = 0; i < BUCKET_SIZE; i++) {
            next = doTryTake(next);
        }
        assertThat(next.tryTake()).isEmpty();

        // Advance time to refill the bucket
        testClock.advanceTime(REFILL_INTERVAL_MS, TimeUnit.MILLISECONDS);
        next = doTryTake(next);
        assertThat(next.tryTake()).isEmpty();
    }

    @Test
    public void testTryTakeRange() throws Exception {
        Pair<Long, ImmutableTokenBucket> next = doTryTake(tokenBucket, 1, BUCKET_SIZE + 1);
        assertThat(next.getLeft()).isEqualTo(BUCKET_SIZE);
        assertThat(next.getRight().tryTake()).isEmpty();

        testClock.advanceTime(10 * REFILL_INTERVAL_MS, TimeUnit.MILLISECONDS);
        next = doTryTake(next.getRight(), 1, BUCKET_SIZE + 1);
        assertThat(next.getLeft()).isEqualTo(BUCKET_SIZE);
        assertThat(next.getRight().tryTake()).isEmpty();
    }

    private ImmutableTokenBucket doTryTake(ImmutableTokenBucket next) {
        next = next.tryTake().orElse(null);
        assertThat(next).describedAs("Expected more tokens in the bucket").isNotNull();
        return next;
    }

    private Pair<Long, ImmutableTokenBucket> doTryTake(ImmutableTokenBucket next, int min, int max) {
        Pair<Long, ImmutableTokenBucket> pair = next.tryTake(min, max).orElse(null);
        assertThat(next).describedAs("Expected more tokens in the bucket").isNotNull();
        return pair;
    }
}