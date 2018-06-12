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
