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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;


public class StepScalingPolicyConfiguration {

    private final Optional<Integer> coolDownSec;
    private final Optional<MetricAggregationType> metricAggregationType;
    private final Optional<StepAdjustmentType> adjustmentType;
    private final Optional<Long> minAdjustmentMagnitude;
    private final List<StepAdjustment> steps;


    public StepScalingPolicyConfiguration(Optional<Integer> coolDownSec, Optional<MetricAggregationType> metricAggregationType,
                                          Optional<StepAdjustmentType> adjustmentType, Optional<Long> minAdjustmentMagnitude,
                                          List<StepAdjustment> steps) {
        this.coolDownSec = coolDownSec;
        this.metricAggregationType = metricAggregationType;
        this.adjustmentType = adjustmentType;
        this.minAdjustmentMagnitude = minAdjustmentMagnitude;
        this.steps = steps;
    }


    public Optional<Integer> getCoolDownSec() { return coolDownSec; }

    public Optional<MetricAggregationType> getMetricAggregationType() {
        return metricAggregationType;
    }

    public Optional<StepAdjustmentType> getAdjustmentType() {
        return adjustmentType;
    }

    public Optional<Long> getMinAdjustmentMagnitude() {
        return minAdjustmentMagnitude;
    }

    public List<StepAdjustment> getSteps() {
        return steps;
    }

    @Override
    public String toString() {
        return "StepScalingPolicyConfiguration{" +
                "coolDownSec=" + coolDownSec +
                ", metricAggregationType=" + metricAggregationType +
                ", adjustmentType=" + adjustmentType +
                ", minAdjustmentMagnitude=" + minAdjustmentMagnitude +
                ", steps=" + steps +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private Optional<Integer> coolDownSec = Optional.empty();
        private Optional<MetricAggregationType> metricAggregationType = Optional.empty();
        private Optional<StepAdjustmentType> adjustmentType = Optional.empty();
        private Optional<Long> minAdjustmentMagnitude = Optional.empty();
        private List<StepAdjustment> steps = new ArrayList<>();

        private Builder() {

        }

        public Builder withCoolDownSec(Integer coolDownSec) {
            this.coolDownSec = Optional.of(coolDownSec);
            return this;
        }

        public Builder withMetricAggregatorType(MetricAggregationType metricAggregatorType) {
            this.metricAggregationType = Optional.of(metricAggregatorType);
            return this;
        }


        public Builder withAdjustmentType(StepAdjustmentType stepAdjustmentType) {
            this.adjustmentType = Optional.of(stepAdjustmentType);
            return this;
        }

        public Builder withMinAdjustmentMagnitude(long minAdjustmentMagnitude) {
            this.minAdjustmentMagnitude = Optional.of(minAdjustmentMagnitude);
            return this;
        }

        public Builder withSteps(List<StepAdjustment> steps) {
            this.steps = steps;
            return this;
        }

        public StepScalingPolicyConfiguration build() {
            return new StepScalingPolicyConfiguration(coolDownSec, metricAggregationType,
                    adjustmentType, minAdjustmentMagnitude, steps);

        }

    }
}
