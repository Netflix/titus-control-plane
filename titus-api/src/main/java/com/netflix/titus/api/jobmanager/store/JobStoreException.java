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

package com.netflix.titus.api.jobmanager.store;

import java.util.Set;

import com.netflix.titus.common.model.sanitizer.ValidationError;

import static java.lang.String.format;

/**
 * A custom runtime exception that indicates an error in the job store.
 */
public class JobStoreException extends RuntimeException {

    public enum ErrorCode {
        CASSANDRA_DRIVER_ERROR,
        BAD_DATA,
        JOB_ALREADY_EXISTS,
        JOB_MUST_BE_ACTIVE,
        JOB_DOES_NOT_EXIST,
        TASK_DOES_NOT_EXIST
    }

    private final ErrorCode errorCode;

    private JobStoreException(String message, ErrorCode errorCode) {
        this(message, errorCode, null);
    }

    private JobStoreException(String message, ErrorCode errorCode, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public static JobStoreException jobMustBeActive(String jobId) {
        return new JobStoreException(format("Job with jobId: %s must be active", jobId), ErrorCode.JOB_MUST_BE_ACTIVE);
    }

    public static JobStoreException jobAlreadyExists(String jobId) {
        return new JobStoreException(format("Job with jobId: %s already exists", jobId), ErrorCode.JOB_ALREADY_EXISTS);
    }

    public static JobStoreException jobDoesNotExist(String jobId) {
        return new JobStoreException(format("Job with jobId: %s does not exist", jobId), ErrorCode.JOB_DOES_NOT_EXIST);
    }

    public static JobStoreException taskDoesNotExist(String taskId) {
        return new JobStoreException(format("Task with taskId: %s does not exist", taskId), ErrorCode.TASK_DOES_NOT_EXIST);
    }

    public static JobStoreException cassandraDriverError(Throwable e) {
        return new JobStoreException(e.getMessage(), ErrorCode.CASSANDRA_DRIVER_ERROR, e);
    }

    public static <T> JobStoreException badData(T value, Set<ValidationError> violations) {
        return new JobStoreException(
                String.format("Entity %s violates constraints: %s", value, violations),
                ErrorCode.BAD_DATA
        );
    }
}
