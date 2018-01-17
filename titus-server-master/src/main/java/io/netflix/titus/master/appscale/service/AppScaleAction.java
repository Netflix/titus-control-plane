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

package io.netflix.titus.master.appscale.service;

import java.util.Optional;

import io.netflix.titus.api.appscale.model.AutoScalingPolicy;

public class AppScaleAction {
    public enum ActionType {
        CREATE_SCALING_POLICY,
        DELETE_SCALING_POLICY,
        UPDATE_SCALABLE_TARGET
    }

    private final ActionType actionType;
    private final String jobId;
    private Optional<AutoScalingPolicy> autoScalingPolicy;
    private Optional<String> policyRefId;

    private AppScaleAction(ActionType actionType, String jobId, AutoScalingPolicy autoScalingPolicy) {
        this.actionType = actionType;
        this.jobId = jobId;
        this.autoScalingPolicy = Optional.of(autoScalingPolicy);
        this.policyRefId = Optional.empty();
    }


    private AppScaleAction(ActionType actionType, String jobId, String policyRefId) {
        this.actionType = actionType;
        this.jobId = jobId;
        this.autoScalingPolicy = Optional.empty();
        this.policyRefId = Optional.of(policyRefId);
    }

    public ActionType getType() {
        return this.actionType;
    }

    public String getJobId() {
        return jobId;
    }

    public Optional<AutoScalingPolicy> getAutoScalingPolicy() {
        return autoScalingPolicy;
    }

    public Optional<String> getPolicyRefId() {
        return policyRefId;
    }


    public static AppScaleActionsBuilder newBuilder() {
        return new AppScaleActionsBuilder();
    }

    public static class AppScaleActionsBuilder {
        private AppScaleActionsBuilder() {
        }

        public AppScaleAction buildCreatePolicyAction(String jobId, AutoScalingPolicy autoScalingPolicy) {
            return new AppScaleAction(ActionType.CREATE_SCALING_POLICY, jobId, autoScalingPolicy);
        }

        public AppScaleAction buildDeletePolicyAction(String jobId, AutoScalingPolicy autoScalingPolicy) {
            return new AppScaleAction(ActionType.DELETE_SCALING_POLICY, jobId, autoScalingPolicy);
        }

        public AppScaleAction buildUpdateTargetAction(String jobId, String policyRefId) {
            return new AppScaleAction(ActionType.UPDATE_SCALABLE_TARGET, jobId, policyRefId);
        }

    }
}
