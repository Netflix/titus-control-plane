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

import java.util.Optional;

import io.netflix.titus.common.util.limiter.tokenbucket.ImmutableTokenBucket;
import io.netflix.titus.common.util.tuple.Pair;

/**
 */
public class DefaultImmutableTokenBucket implements ImmutableTokenBucket {

    private final ImmutableRefillStrategy refillStrategy;

    private final long bucketSize;
    private final long bucketLevel;

    public DefaultImmutableTokenBucket(long bucketSize, long bucketLevel, ImmutableRefillStrategy refillStrategy) {
        this.bucketSize = bucketSize;
        this.bucketLevel = bucketLevel;
        this.refillStrategy = refillStrategy;
    }

    @Override
    public Optional<Pair<Long, ImmutableTokenBucket>> tryTake(long min, long max) {
        if (bucketLevel >= max) {
            return Optional.of(Pair.of(max, new DefaultImmutableTokenBucket(bucketSize, bucketLevel - max, refillStrategy)));
        }

        Pair<ImmutableRefillStrategy, Long> consumed = refillStrategy.consume();
        long refill = consumed.getRight();
        ImmutableRefillStrategy newRefillStrategyInstance = consumed.getLeft();

        if (refill == 0) {
            if (bucketLevel >= min) {
                return Optional.of(Pair.of(bucketLevel, new DefaultImmutableTokenBucket(bucketSize, 0, refillStrategy)));
            }
        } else {
            long newBucketLevel = Math.min(bucketSize, bucketLevel + refill);
            if (newBucketLevel >= min) {
                long tokensToTake = Math.min(newBucketLevel, max);
                return Optional.of(Pair.of(tokensToTake, new DefaultImmutableTokenBucket(bucketSize, newBucketLevel - tokensToTake, newRefillStrategyInstance)));
            }
        }
        return Optional.empty();
    }
}
