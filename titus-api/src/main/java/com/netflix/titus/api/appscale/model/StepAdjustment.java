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

package com.netflix.titus.api.appscale.model;

import java.util.Optional;

public class StepAdjustment {
    private final int scalingAdjustment;
    private final Optional<Double> metricIntervalLowerBound;
    private final Optional<Double> metricIntervalUpperBound;

    public StepAdjustment(int scalingAdjustment, Optional<Double> metricIntervalLowerBound, Optional<Double> metricIntervalUpperBound) {
        this.scalingAdjustment = scalingAdjustment;
        this.metricIntervalLowerBound = metricIntervalLowerBound;
        this.metricIntervalUpperBound = metricIntervalUpperBound;
    }

    public int getScalingAdjustment() {
        return scalingAdjustment;
    }

    public Optional<Double> getMetricIntervalLowerBound() {
        return metricIntervalLowerBound;
    }

    public Optional<Double> getMetricIntervalUpperBound() {
        return metricIntervalUpperBound;
    }

    public static Builder newBuilder() {
        return new Builder();
    }


    public static class Builder {
        private int scalingAdjustment;
        private Optional<Double> metricIntervalLowerBound = Optional.empty();
        private Optional<Double> metricIntervalUpperBound = Optional.empty();

        private Builder() {
        }

        public Builder withScalingAdjustment(int scalingAdjustment) {
            this.scalingAdjustment = scalingAdjustment;
            return this;
        }

        public Builder withMetricIntervalLowerBound(double metricIntervalLowerBound) {
            this.metricIntervalLowerBound = Optional.of(metricIntervalLowerBound);
            return this;
        }

        public Builder withMetricIntervalUpperBound(double metricIntervalUpperBound) {
            this.metricIntervalUpperBound = Optional.of(metricIntervalUpperBound);
            return this;
        }

        public StepAdjustment build() {
            return new StepAdjustment(scalingAdjustment, metricIntervalLowerBound, metricIntervalUpperBound);
        }
    }
}
