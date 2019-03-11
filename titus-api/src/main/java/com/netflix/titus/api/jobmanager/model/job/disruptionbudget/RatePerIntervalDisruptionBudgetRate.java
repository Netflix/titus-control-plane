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

import java.time.Duration;
import java.util.Objects;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

public class RatePerIntervalDisruptionBudgetRate extends DisruptionBudgetRate {

    @Min(1)
    @Max(3600_000)
    private final long intervalMs;

    @Min(1)
    private final int limitPerInterval;

    public RatePerIntervalDisruptionBudgetRate(long intervalMs, int limitPerInterval) {
        this.intervalMs = intervalMs;
        this.limitPerInterval = limitPerInterval;
    }

    public long getIntervalMs() {
        return intervalMs;
    }

    public int getLimitPerInterval() {
        return limitPerInterval;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RatePerIntervalDisruptionBudgetRate that = (RatePerIntervalDisruptionBudgetRate) o;
        return intervalMs == that.intervalMs &&
                limitPerInterval == that.limitPerInterval;
    }

    @Override
    public int hashCode() {
        return Objects.hash(intervalMs, limitPerInterval);
    }

    @Override
    public String toString() {
        return "RatePerIntervalDisruptionBudgetRate{" +
                "intervalMs=" + intervalMs +
                ", limitPerInterval=" + limitPerInterval +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static RatePerIntervalDisruptionBudgetRate relocationRate(Duration interval, int limit) {
        return new RatePerIntervalDisruptionBudgetRate(interval.toMillis(), limit);
    }

    public static final class Builder {
        private long intervalMs;
        private int limitPerInterval;

        private Builder() {
        }

        public Builder withIntervalMs(long intervalMs) {
            this.intervalMs = intervalMs;
            return this;
        }

        public Builder withLimitPerInterval(int limitPerInterval) {
            this.limitPerInterval = limitPerInterval;
            return this;
        }

        public Builder but() {
            return newBuilder().withIntervalMs(intervalMs).withLimitPerInterval(limitPerInterval);
        }

        public RatePerIntervalDisruptionBudgetRate build() {
            return new RatePerIntervalDisruptionBudgetRate(intervalMs, limitPerInterval);
        }
    }
}
