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

package io.netflix.titus.api.appscale.store;

import io.netflix.titus.api.appscale.model.AutoScalingPolicy;
import io.netflix.titus.api.appscale.model.PolicyStatus;
import rx.Completable;
import rx.Observable;

public interface AppScalePolicyStore {

    /**
     * Initialize the store.
     */
    Completable init();

    /**
     * Retrieve all policies
     *
     * @return Observable for AutoScalingPolicy
     */
    Observable<AutoScalingPolicy> retrievePolicies(boolean includeArchived);

    Observable<String> storePolicy(AutoScalingPolicy autoScalingPolicy);

    Completable updatePolicyId(String policyRefId, String policyId);

    Completable updateAlarmId(String policyRefId, String alarmId);

    Completable updatePolicyStatus(String policyRefId, PolicyStatus policyStatus);

    Completable updateStatusMessage(String policyRefId, String statusMessage);

    /**
     * Retrieve auto scaling policies for a Titus Job
     * @param jobId identifies a Titus Job
     * @return Observable for AutoScalingPolicy
     */
    Observable<AutoScalingPolicy> retrievePoliciesForJob(String jobId);


    /**
     * Updates policy configuration for auto scaling policy
     * @param autoScalingPolicy
     * @return
     */
    Completable updatePolicyConfiguration(AutoScalingPolicy autoScalingPolicy);

    Observable<AutoScalingPolicy> retrievePolicyForRefId(String policyRefId);

    Completable removePolicy(String policyRefId);
}
