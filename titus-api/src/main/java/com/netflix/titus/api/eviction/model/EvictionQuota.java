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

import com.netflix.titus.api.model.Tier;
import com.netflix.titus.api.model.reference.Reference;

public class EvictionQuota {

    private final Reference reference;
    private final long quota;

    public EvictionQuota(Reference reference, long quota) {
        this.reference = reference;
        this.quota = quota;
    }

    public Reference getReference() {
        return reference;
    }

    public long getQuota() {
        return quota;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EvictionQuota that = (EvictionQuota) o;
        return quota == that.quota &&
                Objects.equals(reference, that.reference);
    }

    @Override
    public int hashCode() {

        return Objects.hash(reference, quota);
    }

    @Override
    public String toString() {
        return "EvictionQuota{" +
                "reference=" + reference +
                ", quota=" + quota +
                '}';
    }

    public static EvictionQuota emptyQuota(Reference reference) {
        return newBuilder()
                .withReference(reference)
                .withQuota(0L)
                .build();
    }

    public static EvictionQuota systemQuota(long quota) {
        return newBuilder()
                .withReference(Reference.system())
                .withQuota(quota)
                .build();
    }

    public static EvictionQuota tierQuota(Tier tier, int quota) {
        return newBuilder()
                .withReference(Reference.tier(tier))
                .withQuota(quota)
                .build();
    }

    public static EvictionQuota jobQuota(String jobId, long quota) {
        return newBuilder()
                .withReference(Reference.job(jobId))
                .withQuota(quota)
                .build();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private Reference reference;
        private long quota;

        private Builder() {
        }

        public Builder withReference(Reference reference) {
            this.reference = reference;
            return this;
        }

        public Builder withQuota(long quota) {
            this.quota = quota;
            return this;
        }

        public EvictionQuota build() {
            return new EvictionQuota(reference, quota);
        }
    }
}
