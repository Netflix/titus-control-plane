/*
 * Copyright 2019 Netflix, Inc.
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

public class RatePercentagePerIntervalDisruptionBudgetRate extends DisruptionBudgetRate {

    @Min(1)
    @Max(3600_000)
    private final long intervalMs;

    @Min(0)
    @Max(100)
    private final double percentageLimitPerInterval;

    public RatePercentagePerIntervalDisruptionBudgetRate(long intervalMs, double percentageLimitPerInterval) {
        this.intervalMs = intervalMs;
        this.percentageLimitPerInterval = percentageLimitPerInterval;
    }

    public long getIntervalMs() {
        return intervalMs;
    }

    public double getPercentageLimitPerInterval() {
        return percentageLimitPerInterval;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RatePercentagePerIntervalDisruptionBudgetRate that = (RatePercentagePerIntervalDisruptionBudgetRate) o;
        return intervalMs == that.intervalMs &&
                Double.compare(that.percentageLimitPerInterval, percentageLimitPerInterval) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(intervalMs, percentageLimitPerInterval);
    }

    @Override
    public String toString() {
        return "RatePercentagePerIntervalDisruptionBudgetRate{" +
                "intervalMs=" + intervalMs +
                ", percentageLimitPerInterval=" + percentageLimitPerInterval +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {

        private long intervalMs;
        private double percentageLimitPerInterval;

        private Builder() {
        }

        public Builder withIntervalMs(long intervalMs) {
            this.intervalMs = intervalMs;
            return this;
        }

        public Builder withPercentageLimitPerInterval(double percentageLimitPerInterval) {
            this.percentageLimitPerInterval = percentageLimitPerInterval;
            return this;
        }

        public Builder but() {
            return newBuilder().withIntervalMs(intervalMs).withPercentageLimitPerInterval(percentageLimitPerInterval);
        }

        public RatePercentagePerIntervalDisruptionBudgetRate build() {
            return new RatePercentagePerIntervalDisruptionBudgetRate(intervalMs, percentageLimitPerInterval);
        }
    }
}
