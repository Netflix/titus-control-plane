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

package io.netflix.titus.common.util.limiter.tokenbucket;

/**
 * Create a token bucket that can be used to rate limit both synchronously and asynchronously based on the number of
 * available tokens. Tokens will get refilled based on the {@link RefillStrategy}.
 */
public interface TokenBucket {

    /**
     * @return the name of the token bucket.
     */
    String getName();

    /**
     * @return the maximum number of tokens of capacity this bucket can hold.
     */
    long getCapacity();

    /**
     * @return the number of tokens currently in the bucket.
     */
    long getNumberOfTokens();

    /**
     * Attempt to take a token from the bucket.
     *
     * @return true if a token was taken
     */
    boolean tryTake();

    /**
     * Attempt to take a token from the bucket.
     *
     * @param numberOfTokens the number of tokens to take
     * @return true if the number of takens were taken
     */
    boolean tryTake(long numberOfTokens);

    /**
     * Take a token from the bucket and block until the token is taken.
     */
    void take();

    /**
     * Take tokens from the bucket and block until the tokens are taken.
     *
     * @param numberOfTokens the number of tokens to take
     */
    void take(long numberOfTokens);

    /**
     * Refill the token bucket with specified number of tokens. Note that this is an out of bound
     * way to add more tokens to the bucket, but the {@link RefillStrategy} should be doing this.
     *
     * @param numberOfToken the number of tokens to add to the bucket
     */
    void refill(long numberOfToken);

    /**
     * @return the {@link RefillStrategy} of the bucket.
     */
    RefillStrategy getRefillStrategy();

}
