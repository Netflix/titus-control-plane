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

import java.util.Set;

import com.netflix.titus.api.appscale.model.AlarmConfiguration;
import com.netflix.titus.api.appscale.model.AutoScalingPolicy;
import com.netflix.titus.api.appscale.model.PolicyConfiguration;
import com.netflix.titus.api.appscale.model.StepScalingPolicyConfiguration;
import com.netflix.titus.api.appscale.model.TargetTrackingPolicy;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.common.model.sanitizer.ValidationError;
import com.netflix.titus.common.util.CollectionsExt;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class ScalingPolicyModelSanitizationTest {

    @Test
    public void testInvalidScalingConfigurationWithBothPolicies() {
        PolicyConfiguration policyConfiguration = PolicyConfiguration
                .newBuilder()
                .withStepScalingPolicyConfiguration(mock(StepScalingPolicyConfiguration.class))
                .withAlarmConfiguration(mock(AlarmConfiguration.class))
                .withTargetTrackingPolicy(mock(TargetTrackingPolicy.class))
                .build();
        AutoScalingPolicy autoScalingPolicy = AutoScalingPolicy
                .newBuilder()
                .withPolicyConfiguration(policyConfiguration)
                .build();
        Set<ValidationError> validationErrors = newScalingPolicySanitizer().validate(autoScalingPolicy);
        assertThat(validationErrors).hasSize(1);
        assertThat(CollectionsExt.first(validationErrors).getDescription()).isEqualTo("exactly one scaling policy should be set");
    }

    @Test
    public void testInvalidStepScalingPolicyNoAlarmConfiguration() {
        PolicyConfiguration policyConfiguration = PolicyConfiguration
                .newBuilder()
                .withStepScalingPolicyConfiguration(mock(StepScalingPolicyConfiguration.class))
                .build();
        AutoScalingPolicy autoScalingPolicy = AutoScalingPolicy
                .newBuilder()
                .withPolicyConfiguration(policyConfiguration)
                .build();
        Set<ValidationError> validationErrors = newScalingPolicySanitizer().validate(autoScalingPolicy);
        assertThat(validationErrors).hasSize(1);
        assertThat(CollectionsExt.first(validationErrors).getDescription()).isEqualTo("alarmConfiguration must be specified for a step scaling policy");
    }

    private EntitySanitizer newScalingPolicySanitizer() {
        return new ScalingPolicySanitizerBuilder().build();
    }
}
