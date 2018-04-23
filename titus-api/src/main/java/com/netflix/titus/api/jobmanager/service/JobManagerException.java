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

package com.netflix.titus.api.jobmanager.service;

import java.util.List;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobProcesses;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.Tier;

import static java.lang.String.format;

public class JobManagerException extends RuntimeException {

    public enum ErrorCode {
        JobCreateLimited,
        JobNotFound,
        NotServiceJob,
        UnexpectedJobState,
        UnexpectedTaskState,
        TaskNotFound,
        JobTerminating,
        TaskTerminating,
        InvalidContainerResources,
        InvalidDesiredCapacity,
        V2EngineTurnedOff,
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

    /**
     * Returns true, if the argument holds a {@link JobManagerException} instance with an error that may happen during
     * normal execution (for example 'JobNotFound').
     */
    public static boolean isExpected(Throwable error) {
        if (!(error instanceof JobManagerException)) {
            return false;
        }
        switch (((JobManagerException) error).getErrorCode()) {
            case JobCreateLimited:
            case JobNotFound:
            case NotServiceJob:
            case TaskNotFound:
            case JobTerminating:
            case TaskTerminating:
            case InvalidContainerResources:
            case InvalidDesiredCapacity:
                return true;
            case UnexpectedJobState:
            case UnexpectedTaskState:
            case V2EngineTurnedOff:
                return false;
        }
        return false;
    }

    public static boolean hasErrorCode(Throwable error, ErrorCode errorCode) {
        return (error instanceof JobManagerException) && ((JobManagerException) error).getErrorCode() == errorCode;
    }

    public static JobManagerException v2EngineOff() {
        return new JobManagerException(ErrorCode.V2EngineTurnedOff, "V2 engine is turned off. Please, use V3 engine only.");
    }

    public static JobManagerException jobCreateLimited(String violation) {
        return new JobManagerException(ErrorCode.JobCreateLimited, violation);
    }

    public static JobManagerException jobNotFound(String jobId) {
        return new JobManagerException(ErrorCode.JobNotFound, format("Job with id %s does not exist", jobId));
    }

    public static JobManagerException v3JobNotFound(String jobId) {
        return new JobManagerException(ErrorCode.JobNotFound, format("Job with id %s does not exist, or is running on the V2 engine", jobId));
    }

    public static JobManagerException unexpectedJobState(Job job, JobState expectedState) {
        return new JobManagerException(
                ErrorCode.UnexpectedJobState,
                format("Job %s is not in the expected state %s (expected) != %s (actual)", job.getId(), expectedState, job.getStatus().getState())
        );
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

    public static Throwable jobTerminating(Job<?> job) {
        if (job.getStatus().getState() == JobState.Finished) {
            return new JobManagerException(ErrorCode.JobTerminating, format("Job %s is terminated", job.getId()));
        }
        return new JobManagerException(ErrorCode.JobTerminating, format("Job %s is in the termination process", job.getId()));
    }

    public static Throwable taskTerminating(Task task) {
        if (task.getStatus().getState() == TaskState.Finished) {
            return new JobManagerException(ErrorCode.TaskTerminating, format("Task %s is terminated", task.getId()));
        }
        return new JobManagerException(ErrorCode.TaskTerminating, format("Task %s is in the termination process", task.getId()));
    }

    public static JobManagerException invalidContainerResources(Tier tier, ResourceDimension requestedResources, List<ResourceDimension> tierResourceLimits) {
        return new JobManagerException(
                ErrorCode.InvalidContainerResources,
                format("Job too large to run in the %s tier: requested=%s, limits=%s", tier, requestedResources, tierResourceLimits)
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
