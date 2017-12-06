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

package io.netflix.titus.api.jobmanager.service;

import io.netflix.titus.api.jobmanager.model.job.ServiceJobProcesses;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;

import static java.lang.String.format;

public class JobManagerException extends RuntimeException {

    public enum ErrorCode {
        JobCreateLimited,
        JobNotFound,
        NotServiceJob,
        UnexpectedTaskState,
        TaskNotFound,
        InvalidDesiredCapacity,
    }

    private final ErrorCode errorCode;

    private JobManagerException(ErrorCode errorCode, String message) {
        this(errorCode, message, null);
    }

    private JobManagerException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public static JobManagerException jobCreateLimited(String violation) {
        return new JobManagerException(ErrorCode.JobCreateLimited, violation);
    }

    public static JobManagerException jobNotFound(String jobId) {
        return new JobManagerException(ErrorCode.JobNotFound, format("Job with id %s does not exist", jobId));
    }

    public static JobManagerException taskNotFound(String taskId) {
        return new JobManagerException(ErrorCode.TaskNotFound, format("Task with id %s does not exist", taskId));
    }

    public static JobManagerException notServiceJob(String jobId) {
        return new JobManagerException(ErrorCode.NotServiceJob, format("Operation restricted to service jobs, and %s is not the service job", jobId));
    }

    public static JobManagerException unexpectedTaskState(Task task, TaskState expectedState) {
        return new JobManagerException(
                ErrorCode.UnexpectedTaskState,
                format("Task %s is not in the expected state %s (expected) != %s (actual)", task.getId(), expectedState, task.getStatus().getState())
        );
    }

    public static JobManagerException invalidDesiredCapacity(String jobId, int targetDesired, ServiceJobProcesses serviceJobProcesses) {
        return new JobManagerException(
                ErrorCode.InvalidDesiredCapacity,
                format("Job %s can not be updated to desired capacity of %s, disableIncreaseDesired %s, disableDecreaseDesired %s",
                        jobId, targetDesired, serviceJobProcesses.isDisableIncreaseDesired(), serviceJobProcesses.isDisableDecreaseDesired())
        );
    }
}
