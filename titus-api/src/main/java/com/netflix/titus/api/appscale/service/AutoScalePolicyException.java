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

package com.netflix.titus.api.appscale.service;

import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AutoScalePolicyException extends RuntimeException {
    private static Logger log = LoggerFactory.getLogger(AutoScalePolicyException.class);

    public enum ErrorCode {
        JobManagerError,
        InvalidScalingPolicy,
        UnknownScalingPolicy,
        ErrorCreatingPolicy,
        ErrorCreatingTarget,
        ErrorCreatingAlarm,
        ErrorDeletingPolicy,
        ErrorDeletingTarget,
        ErrorDeletingAlarm,
        ErrorUpdatingTargets,
        UnexpectedError,
    }

    private String policyRefId;
    private String jobId;

    private final ErrorCode errorCode;

    private AutoScalePolicyException(ErrorCode errorCode, String message) {
        this(errorCode, message, new RuntimeException(message));
    }

    private AutoScalePolicyException(ErrorCode errorCode, String message, String policyRefId, String jobId) {
        super(message);
        this.policyRefId = policyRefId;
        this.jobId = jobId;
        this.errorCode = errorCode;
    }

    private AutoScalePolicyException(ErrorCode errorCode, String message, String policyRefId) {
        super(message);
        this.policyRefId = policyRefId;
        this.errorCode = errorCode;
    }

    private AutoScalePolicyException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    private AutoScalePolicyException(ErrorCode errorCode, Throwable cause, String policyRefId) {
        super(cause);
        this.errorCode = errorCode;
        this.policyRefId = policyRefId;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }


    public String getJobId() {
        return jobId;
    }

    public String getPolicyRefId() {
        return policyRefId;
    }



    public static AutoScalePolicyException errorCreatingPolicy(String policyRefId, String message) {
        return new AutoScalePolicyException(ErrorCode.ErrorCreatingPolicy, message, policyRefId);
    }

    public static AutoScalePolicyException errorCreatingAlarm(String policyRefId, String message) {
        return new AutoScalePolicyException(ErrorCode.ErrorCreatingAlarm, message, policyRefId);
    }

    public static AutoScalePolicyException errorCreatingTarget(String policyRefId, String jobId, String message) {
        return new AutoScalePolicyException(ErrorCode.ErrorCreatingTarget, message, policyRefId, jobId);
    }


    public static AutoScalePolicyException errorDeletingPolicy(String policyRefId, String message) {
        return new AutoScalePolicyException(ErrorCode.ErrorDeletingPolicy, message, policyRefId);
    }


    public static AutoScalePolicyException errorDeletingAlarm(String policyRefId, String message) {
        return new AutoScalePolicyException(ErrorCode.ErrorDeletingAlarm, message, policyRefId);
    }

    public static AutoScalePolicyException errorDeletingTarget(String policyRefId, String jobId, String message) {
        return new AutoScalePolicyException(ErrorCode.ErrorDeletingTarget, message, policyRefId, jobId);
    }

    public static AutoScalePolicyException errorUpdatingTargets(String policyRefId, String jobId, String message) {
        return new AutoScalePolicyException(ErrorCode.ErrorUpdatingTargets, message, policyRefId, jobId);
    }

    public static AutoScalePolicyException invalidScalingPolicy(String policyRefId, String message) {
        return new AutoScalePolicyException(ErrorCode.InvalidScalingPolicy, message, policyRefId);
    }


    public static AutoScalePolicyException unknownScalingPolicy(String policyRefId, String message) {
        return new AutoScalePolicyException(ErrorCode.UnknownScalingPolicy, message, policyRefId);
    }

    public static AutoScalePolicyException wrapJobManagerException(String policyRefId, JobManagerException jobManagerException) {
        return new AutoScalePolicyException(ErrorCode.JobManagerError, jobManagerException, policyRefId);
    }
}
