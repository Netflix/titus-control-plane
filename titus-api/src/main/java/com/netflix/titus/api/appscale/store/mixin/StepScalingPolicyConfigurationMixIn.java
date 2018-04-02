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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.titus.api.appscale.model.MetricAggregationType;
import com.netflix.titus.api.appscale.model.StepAdjustment;
import com.netflix.titus.api.appscale.model.StepAdjustmentType;
import com.netflix.titus.api.appscale.model.MetricAggregationType;
import com.netflix.titus.api.appscale.model.StepAdjustment;
import com.netflix.titus.api.appscale.model.StepAdjustmentType;

import java.util.List;
import java.util.Optional;


public abstract class StepScalingPolicyConfigurationMixIn {
    @JsonCreator
    StepScalingPolicyConfigurationMixIn(
            @JsonProperty("estimatedInstanceWarmup") Optional<Integer> coolDown,
            @JsonProperty("metricAggregationType") Optional<MetricAggregationType> metricAggregationType,
            @JsonProperty("adjustmentType") Optional<StepAdjustmentType> adjustmentType,
            @JsonProperty("minAdjustmentMagnitude") Optional<Long> minAdjustmentMagnitude,
            @JsonProperty("steps") List<StepAdjustment> steps) {

    }
}
