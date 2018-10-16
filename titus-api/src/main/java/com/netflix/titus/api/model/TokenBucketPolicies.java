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

package com.netflix.titus.api.model;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.util.limiter.Limiters;
import com.netflix.titus.common.util.limiter.tokenbucket.TokenBucket;

/**
 * Supplementary functions to work with the token bucket policies.
 */
public final class TokenBucketPolicies {

    private TokenBucketPolicies() {
    }

    /**
     * Create a new token bucket instance from the provided policy.
     */
    public static TokenBucket newTokenBucket(String name, TokenBucketPolicy policy) {
        Preconditions.checkArgument(
                policy.getRefillPolicy() instanceof FixedIntervalTokenBucketRefillPolicy,
                "Only FixedIntervalTokenBucketRefillPolicy supported"
        );

        FixedIntervalTokenBucketRefillPolicy refillPolicy = (FixedIntervalTokenBucketRefillPolicy) policy.getRefillPolicy();

        return Limiters.createFixedIntervalTokenBucket(
                name,
                policy.getCapacity(),
                policy.getInitialNumberOfTokens(),
                refillPolicy.getNumberOfTokensPerInterval(),
                refillPolicy.getIntervalMs(),
                TimeUnit.MILLISECONDS
        );
    }
}
