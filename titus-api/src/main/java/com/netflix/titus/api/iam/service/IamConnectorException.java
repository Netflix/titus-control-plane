/*
 *
 *  * Copyright 2019 Netflix, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.netflix.titus.api.iam.service;

import static java.lang.String.format;

/**
 * A custom {@link RuntimeException} implementation that indicates errors communicating with a IAM connector.
 */
public class IamConnectorException extends RuntimeException {

    public enum ErrorCode {
        INTERNAL,
        IAM_NOT_FOUND,
        INVALID,
    }

    private final ErrorCode errorCode;

    private IamConnectorException(ErrorCode errorCode, String message) {
        this(errorCode, message, new RuntimeException(message));
    }

    private IamConnectorException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() { return errorCode; }

    public static IamConnectorException iamRoleNotFound(String iamRoleName) {
        return new IamConnectorException(ErrorCode.IAM_NOT_FOUND, format("Could not find IAM %s", iamRoleName));
    }

    public static IamConnectorException iamRoleUnexpectedError(String iamRoleName) {
        return iamRoleUnexpectedError(iamRoleName, "Reason unknown");
    }

    public static IamConnectorException iamRoleUnexpectedError(String iamRoleName, String message) {
        return new IamConnectorException(ErrorCode.INTERNAL, format("Unable to query IAM %s: %s", iamRoleName, message));
    }

    public static IamConnectorException iamRoleCannotAssume(String iamRoleName, String iamAssumeRoleName) {
        return new IamConnectorException(ErrorCode.INVALID, format("Titus cannot assume into role %s: %s unable to assumeRole", iamRoleName, iamAssumeRoleName));
    }
}
