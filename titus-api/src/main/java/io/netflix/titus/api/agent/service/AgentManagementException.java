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

package io.netflix.titus.api.agent.service;

import io.netflix.titus.api.agent.model.AgentInstance;

public class AgentManagementException extends RuntimeException {

    public enum ErrorCode {
        InitializationError,
        InvalidArgument,
        InstanceGroupNotFound,
        AgentNotFound
    }

    private final ErrorCode errorCode;

    public AgentManagementException(ErrorCode errorCode, String message, Throwable cause, Object... args) {
        super(String.format(message, args), cause);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public static AgentManagementException initializationError(String message, Throwable cause, Object... args) {
        return new AgentManagementException(ErrorCode.InitializationError, message, cause, args);
    }

    public static AgentManagementException agentGroupNotFound(String agentGroupId) {
        return new AgentManagementException(ErrorCode.InstanceGroupNotFound, "Instance group %s is not found", null, agentGroupId);
    }

    public static <T> T checkInstanceGroupFound(T serverGroupData, String serverGroupId) {
        if (serverGroupData == null) {
            throw new AgentManagementException(ErrorCode.InstanceGroupNotFound, "Instance group %s is not found", null, serverGroupId);
        }
        return serverGroupData;
    }

    public static AgentManagementException agentNotFound(String agentInstanceId) {
        return new AgentManagementException(ErrorCode.AgentNotFound, "Agent instance %s is not found", null, agentInstanceId);
    }

    public static AgentManagementException invalidArgument(String message, Object... args) {
        throw new AgentManagementException(ErrorCode.InvalidArgument, message, null, args);
    }

    public static AgentInstance checkAgentFound(AgentInstance agentInstance, String agentInstanceId) {
        if (agentInstance == null) {
            throw new AgentManagementException(ErrorCode.AgentNotFound, "Agent instance %s is not found", null, agentInstanceId);
        }
        return agentInstance;
    }

    public static void checkArgument(boolean condition, String message, Object... args) {
        if (!condition) {
            throw new AgentManagementException(ErrorCode.InvalidArgument, message, null, args);
        }
    }
}
