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

public class UnlimitedDisruptionBudgetRate extends DisruptionBudgetRate {

    private static final Builder BUILDER = new Builder();
    private static final UnlimitedDisruptionBudgetRate INSTANCE = new UnlimitedDisruptionBudgetRate();

    public UnlimitedDisruptionBudgetRate() {
    }

    @Override
    public int hashCode() {
        return 12345;
    }

    @Override
    public boolean equals(Object obj) {
        return obj != null && obj.getClass() == UnlimitedDisruptionBudgetRate.class;
    }

    @Override
    public String toString() {
        return "UnlimitedDisruptionBudgetRate{}";
    }

    public static Builder newBuilder() {
        return BUILDER;
    }

    public static final class Builder {

        private Builder() {
        }

        public UnlimitedDisruptionBudgetRate build() {
            return INSTANCE;
        }
    }
}