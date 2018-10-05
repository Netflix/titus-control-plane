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

package com.netflix.titus.api.jobmanager.model.job.disruptionbudget;

import java.util.Objects;

public class SelfManagedDisruptionBudgetPolicy extends DisruptionBudgetPolicy {

    private final long relocationTimeMs;

    public SelfManagedDisruptionBudgetPolicy(long relocationTimeMs) {
        this.relocationTimeMs = relocationTimeMs;
    }

    public long getRelocationTimeMs() {
        return relocationTimeMs;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SelfManagedDisruptionBudgetPolicy that = (SelfManagedDisruptionBudgetPolicy) o;
        return relocationTimeMs == that.relocationTimeMs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(relocationTimeMs);
    }

    @Override
    public String toString() {
        return "SelfManagedDisruptionBudgetPolicy{" +
                "relocationTimeMs=" + relocationTimeMs +
                '}';
    }

    public static final class Builder {
        private long relocationTimeMs;

        private Builder() {
        }

        public Builder withRelocationTimeMs(long relocationTimeMs) {
            this.relocationTimeMs = relocationTimeMs;
            return this;
        }

        public Builder but() {
            return newBuilder().withRelocationTimeMs(relocationTimeMs);
        }

        public SelfManagedDisruptionBudgetPolicy build() {
            return new SelfManagedDisruptionBudgetPolicy(relocationTimeMs);
        }
    }
}
