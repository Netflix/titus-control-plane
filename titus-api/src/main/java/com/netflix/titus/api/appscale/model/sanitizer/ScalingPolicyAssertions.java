/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.api.appscale.model.sanitizer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.netflix.titus.api.appscale.model.AutoScalingPolicy;
import com.netflix.titus.api.appscale.model.PolicyConfiguration;

public class ScalingPolicyAssertions {

    public Map<String, String> validateScalingPolicy(AutoScalingPolicy autoScalingPolicy) {
        if (autoScalingPolicy == null || autoScalingPolicy.getPolicyConfiguration() == null) {
            return Collections.emptyMap();
        }
        PolicyConfiguration policyConfiguration = autoScalingPolicy.getPolicyConfiguration();

        Map<String, String> violations = new HashMap<>();
        if ((policyConfiguration.getStepScalingPolicyConfiguration() != null) == (policyConfiguration.getTargetTrackingPolicy() != null)) {
            violations.put("scalingPolicies", "exactly one scaling policy should be set");
        }
        if (policyConfiguration.getStepScalingPolicyConfiguration() != null && policyConfiguration.getAlarmConfiguration() == null) {
            violations.put("alarmConfiguration", "alarmConfiguration must be specified for a step scaling policy");
        }
        return violations;
    }

}
