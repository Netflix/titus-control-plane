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

package com.netflix.titus.api.appscale.store.mixin;

import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.titus.api.appscale.model.CustomizedMetricSpecification;
import com.netflix.titus.api.appscale.model.PredefinedMetricSpecification;

public class TargetTrackingPolicyMixin {
    @JsonCreator
    TargetTrackingPolicyMixin(
            @JsonProperty("targetValue") double targetValue,
            @JsonProperty("scaleOutCooldownSec") Optional<Integer> scaleOutCooldownSec,
            @JsonProperty("scaleInCooldownSec") Optional<Integer> scaleInCooldownSec,
            @JsonProperty("predefinedMetricSpecification") Optional<PredefinedMetricSpecification> predefinedMetricSpecification,
            @JsonProperty("disableScaleIn") Optional<Boolean> disableScaleIn,
            @JsonProperty("customizedMetricSpecification") Optional<CustomizedMetricSpecification> customizedMetricSpecification) {

    }
}
