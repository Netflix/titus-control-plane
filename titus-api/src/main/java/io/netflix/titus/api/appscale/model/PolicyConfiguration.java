/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.api.appscale.model;

public class PolicyConfiguration {

    private final String name;
    private final PolicyType policyType;
    private final StepScalingPolicyConfiguration stepScalingPolicyConfiguration;
    private final AlarmConfiguration alarmConfiguration;
    private final TargetTrackingPolicy targetTrackingPolicy;

    public PolicyConfiguration(String name, PolicyType policyType, StepScalingPolicyConfiguration stepScalingPolicyConfiguration, AlarmConfiguration alarmConfiguration, TargetTrackingPolicy targetTrackingPolicy) {
        this.name = name;
        this.policyType = policyType;
        this.stepScalingPolicyConfiguration = stepScalingPolicyConfiguration;
        this.alarmConfiguration = alarmConfiguration;
        this.targetTrackingPolicy = targetTrackingPolicy;
    }

    public String getName() {
        return name;
    }

    public PolicyType getPolicyType() {
        return policyType;
    }

    public StepScalingPolicyConfiguration getStepScalingPolicyConfiguration() {
        return stepScalingPolicyConfiguration;
    }

    public AlarmConfiguration getAlarmConfiguration() {
        return alarmConfiguration;
    }

    public TargetTrackingPolicy getTargetTrackingPolicy() {
        return targetTrackingPolicy;
    }

    @Override
    public String toString() {
        return "PolicyConfiguration{" +
                "name='" + name + '\'' +
                ", policyType=" + policyType +
                ", stepScalingPolicyConfiguration=" + stepScalingPolicyConfiguration +
                ", alarmConfiguration=" + alarmConfiguration +
                ", targetTrackingPolicy=" + targetTrackingPolicy +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }


    public static class Builder {
        private String name;
        private PolicyType policyType;
        private StepScalingPolicyConfiguration stepScalingPolicyConfiguration;
        private AlarmConfiguration alarmConfiguration;
        private TargetTrackingPolicy targetTrackingPolicy;

        private Builder() {
        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withPolicyType(PolicyType policyType) {
            this.policyType = policyType;
            return this;
        }

        public Builder withStepScalingPolicyConfiguration(StepScalingPolicyConfiguration stepScalingPolicyConfiguration) {
            this.stepScalingPolicyConfiguration = stepScalingPolicyConfiguration;
            return this;
        }

        public Builder withAlarmConfiguration(AlarmConfiguration alarmConfiguration) {
            this.alarmConfiguration = alarmConfiguration;
            return this;
        }

        public Builder withTargetTrackingPolicy(TargetTrackingPolicy targetTrackingPolicy) {
            this.targetTrackingPolicy = targetTrackingPolicy;
            return this;
        }

        public PolicyConfiguration build() {
            return new PolicyConfiguration(name, policyType, stepScalingPolicyConfiguration, alarmConfiguration, targetTrackingPolicy);
        }
    }
}
