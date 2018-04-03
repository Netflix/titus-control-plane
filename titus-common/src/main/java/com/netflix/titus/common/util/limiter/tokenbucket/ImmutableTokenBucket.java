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

package com.netflix.titus.common.util.limiter.tokenbucket;

import java.util.Optional;

import com.netflix.titus.common.util.tuple.Pair;

/**
 */
public interface ImmutableTokenBucket {

    interface ImmutableRefillStrategy {
        Pair<ImmutableRefillStrategy, Long> consume();
    }

    /**
     * Attempt to take a token from the bucket.
     *
     * @return {@link Optional#empty()} if not enough tokens available, or {@link ImmutableTokenBucket} containing one token less
     */
    default Optional<ImmutableTokenBucket> tryTake() {
        return tryTake(1);
    }

    /**
     * Attempt to take a token from the bucket.
     *
     * @param numberOfTokens the number of tokens to take
     * @return {@link Optional#empty()} if not enough tokens available, or {@link ImmutableTokenBucket} with 'numberOfTokens' less
     */
    default Optional<ImmutableTokenBucket> tryTake(long numberOfTokens) {
        return tryTake(numberOfTokens, numberOfTokens).map(Pair::getRight);
    }

    /**
     * Attempt to take at least min number of tokens, up to max if more available.
     *
     * @return {@link Optional#empty()} if less then min tokens available, or a pir of values with the first one
     * being number of tokens consumed, and the second one {@link ImmutableTokenBucket} with the tokens consumed
     */
    Optional<Pair<Long, ImmutableTokenBucket>> tryTake(long min, long max);
}
