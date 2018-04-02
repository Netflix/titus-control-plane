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
import com.netflix.titus.api.appscale.model.PolicyConfiguration;
import com.netflix.titus.api.appscale.model.PolicyStatus;

public class AutoScalingPolicyMixIn {
    @JsonCreator
    AutoScalingPolicyMixIn(
            @JsonProperty("refId") String refId,
            @JsonProperty("jobId") String jobId,
            @JsonProperty("policyConfiguration") PolicyConfiguration policyConfiguration,
            @JsonProperty("policyId") String policyId,
            @JsonProperty("alarmId") String alarmId,
            @JsonProperty("status") PolicyStatus status) {

    }
}
