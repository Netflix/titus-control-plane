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

public class TargetTrackingPolicy {
    private final double targetValue;
    private final Optional<Integer> scaleOutCooldownSec;
    private final Optional<Integer> scaleInCooldownSec;
    private final Optional<PredefinedMetricSpecification> predefinedMetricSpecification;
    private final Optional<Boolean> disableScaleIn;
    private final Optional<CustomizedMetricSpecification> customizedMetricSpecification;

    public TargetTrackingPolicy(double targetValue, Optional<Integer> scaleOutCooldownSec, Optional<Integer> scaleInCooldownSec, Optional<PredefinedMetricSpecification> predefinedMetricSpecification, Optional<Boolean> disableScaleIn, Optional<CustomizedMetricSpecification> customizedMetricSpecification) {
        this.targetValue = targetValue;
        this.scaleOutCooldownSec = scaleOutCooldownSec;
        this.scaleInCooldownSec = scaleInCooldownSec;
        this.predefinedMetricSpecification = predefinedMetricSpecification;
        this.disableScaleIn = disableScaleIn;
        this.customizedMetricSpecification = customizedMetricSpecification;
    }

    public double getTargetValue() {
        return targetValue;
    }

    public Optional<Integer> getScaleOutCooldownSec() {
        return scaleOutCooldownSec;
    }

    public Optional<Integer> getScaleInCooldownSec() {
        return scaleInCooldownSec;
    }

    public Optional<PredefinedMetricSpecification> getPredefinedMetricSpecification() {
        return predefinedMetricSpecification;
    }

    public Optional<Boolean> getDisableScaleIn() {
        return disableScaleIn;
    }

    public Optional<CustomizedMetricSpecification> getCustomizedMetricSpecification() {
        return customizedMetricSpecification;
    }

    @Override
    public String toString() {
        return "TargetTrackingPolicy{" +
                "targetValue=" + targetValue +
                ", scaleOutCooldownSec=" + scaleOutCooldownSec +
                ", scaleInCooldownSec=" + scaleInCooldownSec +
                ", predefinedMetricSpecification=" + predefinedMetricSpecification +
                ", disableScaleIn=" + disableScaleIn +
                ", customizedMetricSpecification=" + customizedMetricSpecification +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private double targetValue;
        private Optional<Integer> scaleOutCooldownSec = Optional.empty();
        private Optional<Integer> scaleInCooldownSec = Optional.empty();
        private Optional<PredefinedMetricSpecification> predefinedMetricSpecification = Optional.empty();
        private Optional<Boolean> disableScaleIn = Optional.empty();
        private Optional<CustomizedMetricSpecification> customizedMetricSpecification = Optional.empty();

        private Builder() {
        }

        public static Builder aTargetTrackingPolicy() {
            return new Builder();
        }

        public Builder withTargetValue(double targetValue) {
            this.targetValue = targetValue;
            return this;
        }

        public Builder withScaleOutCooldownSec(Integer scaleOutCooldownSec) {
            this.scaleOutCooldownSec = Optional.of(scaleOutCooldownSec);
            return this;
        }

        public Builder withScaleInCooldownSec(Integer scaleInCooldownSec) {
            this.scaleInCooldownSec = Optional.of(scaleInCooldownSec);
            return this;
        }

        public Builder withPredefinedMetricSpecification(PredefinedMetricSpecification predefinedMetricSpecification) {
            this.predefinedMetricSpecification = Optional.of(predefinedMetricSpecification);
            return this;
        }

        public Builder withDisableScaleIn(Boolean disableScaleIn) {
            this.disableScaleIn = Optional.of(disableScaleIn);
            return this;
        }

        public Builder withCustomizedMetricSpecification(CustomizedMetricSpecification customizedMetricSpecification) {
            this.customizedMetricSpecification = Optional.of(customizedMetricSpecification);
            return this;
        }

        public TargetTrackingPolicy build() {
            return new TargetTrackingPolicy(targetValue, scaleOutCooldownSec, scaleInCooldownSec, predefinedMetricSpecification, disableScaleIn, customizedMetricSpecification);
        }
    }
}
