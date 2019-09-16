/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.api.clustermembership.connector;

public class ClusterMembershipConnectorException extends RuntimeException {

    public enum ErrorCode {
        Conflict,
        ClientError,
    }

    private final ErrorCode errorCode;

    private ClusterMembershipConnectorException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public static ClusterMembershipConnectorException clientError(String message, Throwable cause) {
        return new ClusterMembershipConnectorException(
                ErrorCode.ClientError,
                String.format("Error during communicating with the cluster membership client: %s", message),
                cause
        );
    }

    public static ClusterMembershipConnectorException conflict(String message, Throwable cause) {
        return new ClusterMembershipConnectorException(
                ErrorCode.Conflict,
                String.format("Request conflicts with the current sate: %s", message),
                cause
        );
    }
}
