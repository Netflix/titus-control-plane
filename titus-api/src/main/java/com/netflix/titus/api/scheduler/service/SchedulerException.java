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

package com.netflix.titus.api.scheduler.service;

import com.netflix.titus.api.scheduler.model.SystemSelector;

public class SchedulerException extends RuntimeException {

    public enum ErrorCode {
        InvalidArgument,
        SystemSelectorInitializationError,
        SystemSelectorAlreadyExists,
        SystemSelectorNotFound,
        SystemSelectorEvaluationError
    }

    private final ErrorCode errorCode;

    public SchedulerException(ErrorCode errorCode, String message, Throwable cause, Object... args) {
        super(String.format(message, args), cause);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public static SchedulerException invalidArgument(String message, Object... args) {
        throw new SchedulerException(ErrorCode.InvalidArgument, message, null, args);
    }

    public static SchedulerException systemSelectorInitializationError(String message, Throwable cause, Object... args) {
        return new SchedulerException(ErrorCode.SystemSelectorInitializationError, message, cause, args);
    }

    public static SchedulerException systemSelectorEvaluationError(String message, Throwable cause, Object... args) {
        return new SchedulerException(ErrorCode.SystemSelectorEvaluationError, message, cause, args);
    }

    public static void checkSystemSelectorAlreadyExists(SystemSelector systemSelector, String systemSelectorId) {
        if (systemSelector != null) {
            throw new SchedulerException(ErrorCode.SystemSelectorAlreadyExists, "System selector %s already exists", null, systemSelectorId);
        }
    }

    public static SystemSelector checkSystemSelectorFound(SystemSelector systemSelector, String systemSelectorId) {
        if (systemSelector == null) {
            throw new SchedulerException(ErrorCode.SystemSelectorNotFound, "System selector %s not found", null, systemSelectorId);
        }
        return systemSelector;
    }

    public static void checkArgument(boolean condition, String message, Object... args) {
        if (!condition) {
            throw new SchedulerException(ErrorCode.InvalidArgument, message, null, args);
        }
    }
}
