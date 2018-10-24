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

package com.netflix.titus.ext.aws.appscale;

import com.amazonaws.AmazonServiceException;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;

public class AWSAppAutoScalingMetrics {
    private final Registry registry;


    private static final String METRIC_APP_SCALE_CREATE_TARGET = "titus.appscale.create.target";
    private static final String METRIC_APP_SCALE_CREATE_POLICY = "titus.appscale.create.policy";
    private static final String METRIC_APP_SCALE_DELETE_TARGET = "titus.appscale.delete.target";
    private static final String METRIC_APP_SCALE_DELETE_POLICY = "titus.appscale.delete.policy";
    private static final String METRIC_APP_SCALE_GET_TARGET = "titus.appscale.get.target";

    private static final String METRIC_APP_SCALE_CREATE_TARGET_ERROR = "titus.appscale.create.target.error";
    private static final String METRIC_APP_SCALE_CREATE_POLICY_ERROR = "titus.appscale.create.policy.error";
    private static final String METRIC_APP_SCALE_DELETE_TARGET_ERROR = "titus.appscale.delete.target.error";
    private static final String METRIC_APP_SCALE_DELETE_POLICY_ERROR = "titus.appscale.delete.policy.error";
    private static final String METRIC_APP_SCALE_GET_TARGET_ERROR = "titus.appscale.get.target.error";

    private static final String METRIC_ERR_CODE_UNKNOWN = "unknown";
    private static final String METRIC_ERR_CODE_TAG = "errorCode";

    private final Id createTargetId;
    private final Id deleteTargetId;
    private final Id getTargetId;
    private final Id createPolicyId;
    private final Id deletePolicyId;

    private final Id createTargetErrorId;
    private final Id deleteTargetErrorId;
    private final Id getTargetErrorId;
    private final Id createPolicyErrorId;
    private final Id deletePolicyErrorId;

    public AWSAppAutoScalingMetrics(Registry registry) {
        this.registry = registry;

        createTargetId = registry.createId(METRIC_APP_SCALE_CREATE_TARGET);
        deleteTargetId = registry.createId(METRIC_APP_SCALE_DELETE_TARGET);
        getTargetId = registry.createId(METRIC_APP_SCALE_GET_TARGET);

        createPolicyId = registry.createId(METRIC_APP_SCALE_CREATE_POLICY);
        deletePolicyId = registry.createId(METRIC_APP_SCALE_DELETE_POLICY);

        createTargetErrorId = registry.createId(METRIC_APP_SCALE_CREATE_TARGET_ERROR);
        deleteTargetErrorId = registry.createId(METRIC_APP_SCALE_DELETE_TARGET_ERROR);
        getTargetErrorId = registry.createId(METRIC_APP_SCALE_GET_TARGET_ERROR);

        createPolicyErrorId = registry.createId(METRIC_APP_SCALE_CREATE_POLICY_ERROR);
        deletePolicyErrorId = registry.createId(METRIC_APP_SCALE_DELETE_POLICY_ERROR);
    }

    void registerAwsCreateTargetSuccess() {
        registry.counter(createTargetId).increment();
    }

    void registerAwsCreateTargetError(Exception exception) {
        registry.counter(createTargetErrorId
                .withTag(METRIC_ERR_CODE_TAG, getErrorCode(exception))
        ).increment();
    }

    void registerAwsDeleteTargetSuccess() {
        registry.counter(deleteTargetId).increment();
    }

    void registerAwsDeleteTargetError(Exception exception) {
        registry.counter(deleteTargetErrorId
                .withTag(METRIC_ERR_CODE_TAG, getErrorCode(exception))
        ).increment();
    }

    void registerAwsGetTargetSuccess() {
        registry.counter(getTargetId).increment();
    }

    void registerAwsGetTargetError(Exception exception) {
        registry.counter(getTargetErrorId
                .withTag(METRIC_ERR_CODE_TAG, getErrorCode(exception))
        ).increment();
    }

    void registerAwsCreatePolicySuccess() {
        registry.counter(createPolicyId).increment();
    }

    void registerAwsCreatePolicyError(Exception exception) {
        registry.counter(createPolicyErrorId
                .withTag(METRIC_ERR_CODE_TAG, getErrorCode(exception))
        ).increment();
    }

    void registerAwsDeletePolicySuccess() {
        registry.counter(deletePolicyId).increment();
    }

    void registerAwsDeletePolicyError(Exception exception) {
        registry.counter(deletePolicyErrorId
                .withTag(METRIC_ERR_CODE_TAG, getErrorCode(exception))
        ).increment();
    }

    private String getErrorCode(Exception exception) {
        String errCode = METRIC_ERR_CODE_UNKNOWN;
        if (exception instanceof AmazonServiceException) {
            errCode = ((AmazonServiceException)exception).getErrorCode();
        }
        return errCode;
    }
}
