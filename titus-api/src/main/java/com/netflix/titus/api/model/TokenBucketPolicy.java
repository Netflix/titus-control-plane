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

import java.util.Objects;

public class TokenBucketPolicy {

    private final long capacity;
    private final long initialNumberOfTokens;
    private final TokenBucketRefillPolicy refillPolicy;

    public TokenBucketPolicy(long capacity, long initialNumberOfTokens, TokenBucketRefillPolicy refillPolicy) {
        this.capacity = capacity;
        this.initialNumberOfTokens = initialNumberOfTokens;
        this.refillPolicy = refillPolicy;
    }

    public long getCapacity() {
        return capacity;
    }

    public long getInitialNumberOfTokens() {
        return initialNumberOfTokens;
    }

    public TokenBucketRefillPolicy getRefillPolicy() {
        return refillPolicy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TokenBucketPolicy that = (TokenBucketPolicy) o;
        return capacity == that.capacity &&
                initialNumberOfTokens == that.initialNumberOfTokens &&
                Objects.equals(refillPolicy, that.refillPolicy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(capacity, initialNumberOfTokens, refillPolicy);
    }

    @Override
    public String toString() {
        return "TokenBucketPolicy{" +
                "capacity=" + capacity +
                ", initialNumberOfTokens=" + initialNumberOfTokens +
                ", refillPolicy=" + refillPolicy +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private long capacity;
        private long initialNumberOfTokens;
        private TokenBucketRefillPolicy refillPolicy;

        private Builder() {
        }

        public Builder withCapacity(long capacity) {
            this.capacity = capacity;
            return this;
        }

        public Builder withInitialNumberOfTokens(long initialNumberOfTokens) {
            this.initialNumberOfTokens = initialNumberOfTokens;
            return this;
        }

        public Builder withRefillPolicy(TokenBucketRefillPolicy refillPolicy) {
            this.refillPolicy = refillPolicy;
            return this;
        }

        public TokenBucketPolicy build() {
            return new TokenBucketPolicy(capacity, initialNumberOfTokens, refillPolicy);
        }
    }
}
