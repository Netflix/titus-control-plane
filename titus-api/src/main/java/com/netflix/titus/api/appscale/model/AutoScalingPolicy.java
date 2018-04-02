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

public class AutoScalingPolicy {
    private final String refId;
    private final PolicyConfiguration policyConfiguration;

    private final String jobId;
    private final String policyId;
    private final String alarmId;
    private final PolicyStatus status;
    private final String statusMessage;

    public AutoScalingPolicy(String refId, String jobId,
                             PolicyConfiguration policyConfiguration, String policyId,
                             String alarmId, PolicyStatus status, String statusMessage) {
        this.refId = refId;
        this.jobId = jobId;
        this.policyConfiguration = policyConfiguration;
        this.alarmId = alarmId;
        this.status = status;
        this.policyId = policyId;
        this.statusMessage = statusMessage;
    }

    public String getJobId() {
        return jobId;
    }

    public String getRefId() {
        return refId;
    }

    public PolicyConfiguration getPolicyConfiguration() {
        return policyConfiguration;
    }

    public String getPolicyId() {
        return policyId;
    }

    public String getAlarmId() {
        return alarmId;
    }

    public PolicyStatus getStatus() {
        return status;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "AutoScalingPolicy{" +
                "refId='" + refId + '\'' +
                ", policyConfiguration=" + policyConfiguration +
                ", policyId='" + policyId + '\'' +
                ", alarmId='" + alarmId + '\'' +
                ", status=" + status +
                '}';
    }

    public static class Builder {
        private String refId;
        private String jobId;
        private PolicyConfiguration policyConfiguration;

        private String policyId;
        private String alarmId;
        private PolicyStatus status;
        private String statusMessage;

        private Builder() {
        }

        public Builder withAutoScalingPolicy(AutoScalingPolicy other) {
            this.refId = other.refId;
            this.jobId = other.jobId;
            this.policyConfiguration = other.policyConfiguration; // shallow copy OK since immutable
            this.alarmId = other.alarmId;
            this.status = other.status;
            this.policyId = other.policyId;
            this.statusMessage = other.statusMessage;
            return this;
        }

        public Builder withJobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder withRefId(String refId) {
            this.refId = refId;
            return this;
        }

        public Builder withPolicyConfiguration(PolicyConfiguration policyConfiguration) {
            this.policyConfiguration = policyConfiguration;
            return this;
        }

        public Builder withPolicyId(String policyId) {
            this.policyId = policyId;
            return this;
        }

        public Builder withAlarmId(String alarmId) {
            this.alarmId = alarmId;
            return this;
        }

        public Builder withStatus(PolicyStatus policyStatus) {
            this.status= policyStatus;
            return this;
        }

        public Builder withStatusMessage(String statusMessage) {
            this.statusMessage = statusMessage;
            return this;
        }

        public AutoScalingPolicy build() {
            return new AutoScalingPolicy(refId, jobId, policyConfiguration, policyId, alarmId, status, statusMessage);
        }
    }

}

