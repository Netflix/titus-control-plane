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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.TimeWindow;
import com.netflix.titus.api.model.FixedIntervalTokenBucketRefillPolicy;
import com.netflix.titus.api.model.TokenBucketPolicy;
import com.netflix.titus.api.model.reference.Reference;

public class SystemDisruptionBudget {

    private final Reference reference;
    private final TokenBucketPolicy tokenBucketPolicy;
    private final List<TimeWindow> timeWindows;

    public SystemDisruptionBudget(Reference reference, TokenBucketPolicy tokenBucketPolicy, List<TimeWindow> timeWindows) {
        this.reference = reference;
        this.tokenBucketPolicy = tokenBucketPolicy;
        this.timeWindows = timeWindows;
    }

    public Reference getReference() {
        return reference;
    }

    public TokenBucketPolicy getTokenBucketPolicy() {
        return tokenBucketPolicy;
    }

    public List<TimeWindow> getTimeWindows() {
        return timeWindows;
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
                Objects.equals(tokenBucketPolicy, that.tokenBucketPolicy) &&
                Objects.equals(timeWindows, that.timeWindows);
    }

    @Override
    public int hashCode() {
        return Objects.hash(reference, tokenBucketPolicy, timeWindows);
    }

    @Override
    public String toString() {
        return "SystemDisruptionBudget{" +
                "reference=" + reference +
                ", tokenBucketPolicy=" + tokenBucketPolicy +
                ", timeWindows=" + timeWindows +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder().withReference(reference).withTokenBucketDescriptor(tokenBucketPolicy);
    }

    public static SystemDisruptionBudget newBasicSystemDisruptionBudget(long refillRatePerSecond, long capacity, TimeWindow... timeWindows) {
        return newBuilder()
                .withReference(Reference.system())
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
                .withTimeWindows(timeWindows.length == 0 ? Collections.emptyList() : Arrays.asList(timeWindows))
                .build();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private Reference reference;
        private TokenBucketPolicy tokenBucketPolicy;
        private List<TimeWindow> timeWindows;

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

        public Builder withTimeWindows(List<TimeWindow> timeWindows) {
            this.timeWindows = timeWindows;
            return this;
        }

        public SystemDisruptionBudget build() {
            Preconditions.checkNotNull(reference, "Reference is null");
            Preconditions.checkNotNull(tokenBucketPolicy, "Token bucket is null");
            Preconditions.checkNotNull(timeWindows, "Time windows collection is null");

            return new SystemDisruptionBudget(reference, tokenBucketPolicy, timeWindows);
        }
    }
}
