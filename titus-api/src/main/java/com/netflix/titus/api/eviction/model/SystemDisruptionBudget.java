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

package com.netflix.titus.api.eviction.model;

import java.util.Objects;

import com.netflix.titus.api.model.FixedIntervalTokenBucketRefillPolicy;
import com.netflix.titus.api.model.TokenBucketPolicy;
import com.netflix.titus.api.model.reference.Reference;

public class SystemDisruptionBudget {

    private final Reference reference;
    private final TokenBucketPolicy tokenBucketPolicy;

    public SystemDisruptionBudget(Reference reference, TokenBucketPolicy tokenBucketPolicy) {
        this.reference = reference;
        this.tokenBucketPolicy = tokenBucketPolicy;
    }

    public Reference getReference() {
        return reference;
    }

    public TokenBucketPolicy getTokenBucketPolicy() {
        return tokenBucketPolicy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SystemDisruptionBudget that = (SystemDisruptionBudget) o;
        return Objects.equals(reference, that.reference) &&
                Objects.equals(tokenBucketPolicy, that.tokenBucketPolicy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(reference, tokenBucketPolicy);
    }

    @Override
    public String toString() {
        return "SystemDisruptionBudget{" +
                "reference=" + reference +
                ", tokenBucketPolicy=" + tokenBucketPolicy +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder().withReference(reference).withTokenBucketDescriptor(tokenBucketPolicy);
    }

    public static SystemDisruptionBudget newBasicSystemDisruptionBudget(long refillRatePerSecond, long capacity) {
        return newBuilder()
                .withReference(Reference.global())
                .withTokenBucketDescriptor(TokenBucketPolicy.newBuilder()
                        .withInitialNumberOfTokens(0)
                        .withCapacity(capacity)
                        .withRefillPolicy(FixedIntervalTokenBucketRefillPolicy.newBuilder()
                                .withNumberOfTokensPerInterval(refillRatePerSecond)
                                .withIntervalMs(1_000)
                                .build()
                        )
                        .build()
                )
                .build();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private Reference reference;
        private TokenBucketPolicy tokenBucketPolicy;

        private Builder() {
        }

        public Builder withReference(Reference reference) {
            this.reference = reference;
            return this;
        }

        public Builder withTokenBucketDescriptor(TokenBucketPolicy tokenBucketPolicy) {
            this.tokenBucketPolicy = tokenBucketPolicy;
            return this;
        }

        public SystemDisruptionBudget build() {
            return new SystemDisruptionBudget(reference, tokenBucketPolicy);
        }
    }
}
