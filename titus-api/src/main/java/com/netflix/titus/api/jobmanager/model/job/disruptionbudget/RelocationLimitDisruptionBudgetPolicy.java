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

public class RelocationLimitDisruptionBudgetPolicy extends DisruptionBudgetPolicy {

    private final int limit;

    public RelocationLimitDisruptionBudgetPolicy(int limit) {
        this.limit = limit;
    }

    public int getLimit() {
        return limit;
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
        RelocationLimitDisruptionBudgetPolicy that = (RelocationLimitDisruptionBudgetPolicy) o;
        return limit == that.limit;
    }

    @Override
    public int hashCode() {
        return Objects.hash(limit);
    }

    @Override
    public String toString() {
        return "RelocationLimitDisruptionBudgetPolicy{" +
                "limit=" + limit +
                '}';
    }

    public static final class Builder {
        private int limit;

        private Builder() {
        }

        public Builder withLimit(int limit) {
            this.limit = limit;
            return this;
        }

        public Builder but() {
            return newBuilder().withLimit(limit);
        }

        public RelocationLimitDisruptionBudgetPolicy build() {
            return new RelocationLimitDisruptionBudgetPolicy(limit);
        }
    }
}
