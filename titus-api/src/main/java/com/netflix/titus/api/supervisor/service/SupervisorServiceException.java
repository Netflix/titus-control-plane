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

package com.netflix.titus.api.supervisor.service;

import com.netflix.titus.api.supervisor.model.MasterInstance;

import static java.lang.String.format;

public class SupervisorServiceException extends RuntimeException {

    public enum ErrorCode {
        MasterInstanceNotFound,
        NotLeader,
    }

    private final ErrorCode errorCode;

    private SupervisorServiceException(ErrorCode errorCode, String message) {
        this(errorCode, message, null);
    }

    private SupervisorServiceException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public static SupervisorServiceException masterInstanceNotFound(String instanceId) {
        return new SupervisorServiceException(ErrorCode.MasterInstanceNotFound, format("TitusMaster instance with id %s does not exist", instanceId));
    }

    public static SupervisorServiceException notLeader(MasterInstance currentMasterInstance) {
        return new SupervisorServiceException(ErrorCode.NotLeader, String.format("TitusMaster instance %s is not a leader", currentMasterInstance.getInstanceId()));
    }
}
