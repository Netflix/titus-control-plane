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

package com.netflix.titus.api.connector.cloud;

import java.util.Collection;

public class CloudConnectorException extends RuntimeException {

    public enum ErrorCode {
        Internal,
        InvalidData,
        NotFound,
    }

    private final ErrorCode errorCode;

    private CloudConnectorException(ErrorCode errorCode, String message, Object[] args) {
        super(String.format(message, args));
        this.errorCode = errorCode;
    }

    private CloudConnectorException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public static boolean isThis(Throwable cause, ErrorCode errorCode) {
        return cause instanceof CloudConnectorException && ((CloudConnectorException) cause).getErrorCode() == errorCode;
    }

    public static CloudConnectorException internalError(String message, Object... args) {
        return new CloudConnectorException(ErrorCode.Internal, message, args);
    }

    public static CloudConnectorException invalidInstanceGroupId(Collection<String> invalidIds) {
        return new CloudConnectorException(ErrorCode.NotFound, "Invalid instance group id(s): " + invalidIds);
    }

    public static CloudConnectorException unrecognizedInstanceType(String instanceType) {
        return new CloudConnectorException(ErrorCode.NotFound, "Unrecognized instance type:  " + instanceType);
    }

    public static CloudConnectorException invalidArgument(String message, Object... args) {
        return new CloudConnectorException(ErrorCode.InvalidData, message, args);
    }

    public static CloudConnectorException unrecognizedTargetGroup(String targetGroup) {
        return new CloudConnectorException(ErrorCode.NotFound, "Unrecognized target group:  " + targetGroup);
    }

    public static void checkArgument(boolean isValid, String message, Object... args) {
        if (isValid) {
            throw new CloudConnectorException(ErrorCode.InvalidData, message, args);
        }
    }
}
