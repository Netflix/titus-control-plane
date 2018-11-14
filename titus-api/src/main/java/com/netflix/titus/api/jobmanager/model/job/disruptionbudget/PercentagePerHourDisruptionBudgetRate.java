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
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

public class PercentagePerHourDisruptionBudgetRate extends DisruptionBudgetRate {

    @Min(0)
    @Max(100)
    private final double maxPercentageOfContainersRelocatedInHour;

    public PercentagePerHourDisruptionBudgetRate(double maxPercentageOfContainersRelocatedInHour) {
        this.maxPercentageOfContainersRelocatedInHour = maxPercentageOfContainersRelocatedInHour;
    }

    public double getMaxPercentageOfContainersRelocatedInHour() {
        return maxPercentageOfContainersRelocatedInHour;
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
        PercentagePerHourDisruptionBudgetRate that = (PercentagePerHourDisruptionBudgetRate) o;
        return Double.compare(that.maxPercentageOfContainersRelocatedInHour, maxPercentageOfContainersRelocatedInHour) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxPercentageOfContainersRelocatedInHour);
    }

    @Override
    public String toString() {
        return "PercentagePerHourDisruptionBudgetRate{" +
                "maxPercentageOfContainersRelocatedInHour=" + maxPercentageOfContainersRelocatedInHour +
                '}';
    }

    public static final class Builder {
        private double maxPercentageOfContainersRelocatedInHour;

        private Builder() {
        }

        public Builder withMaxPercentageOfContainersRelocatedInHour(double maxPercentageOfContainersRelocatedInHour) {
            this.maxPercentageOfContainersRelocatedInHour = maxPercentageOfContainersRelocatedInHour;
            return this;
        }

        public Builder but() {
            return newBuilder().withMaxPercentageOfContainersRelocatedInHour(maxPercentageOfContainersRelocatedInHour);
        }

        public PercentagePerHourDisruptionBudgetRate build() {
            return new PercentagePerHourDisruptionBudgetRate(maxPercentageOfContainersRelocatedInHour);
        }
    }
}
