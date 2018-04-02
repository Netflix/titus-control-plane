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
import com.netflix.titus.api.appscale.model.AlarmConfiguration;
import com.netflix.titus.api.appscale.model.PolicyType;
import com.netflix.titus.api.appscale.model.StepScalingPolicyConfiguration;
import com.netflix.titus.api.appscale.model.TargetTrackingPolicy;
import com.netflix.titus.api.appscale.model.AlarmConfiguration;
import com.netflix.titus.api.appscale.model.PolicyType;
import com.netflix.titus.api.appscale.model.StepScalingPolicyConfiguration;
import com.netflix.titus.api.appscale.model.TargetTrackingPolicy;

public class PolicyConfigurationMixIn {
    @JsonCreator
    PolicyConfigurationMixIn(
            @JsonProperty("name") String name,
            @JsonProperty("policyType") PolicyType policyType,
            @JsonProperty("stepScalingPolicyConfiguration") StepScalingPolicyConfiguration stepScalingPolicyConfiguration,
            @JsonProperty("alarmConfiguration") AlarmConfiguration alarmConfiguration,
            @JsonProperty("targetTrackingPolicy") TargetTrackingPolicy targetTrackingPolicy) {
    }
}
