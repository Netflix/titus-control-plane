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

package com.netflix.titus.api.eviction.service;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;

public class EvictionException extends RuntimeException {

    private final ErrorCode errorCode;

    public enum ErrorCode {
        BadConfiguration,
        CapacityGroupNotFound,
        TaskNotFound,
        TaskNotScheduledYet,
        TaskAlreadyStopped,
        NoSystemQuota,
        NoJobQuota,
    }

    private EvictionException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public static EvictionException badConfiguration(String reason, Object... args) {
        return new EvictionException(ErrorCode.BadConfiguration, String.format("Bad configuration data: %s", String.format(reason, args)));
    }

    public static EvictionException taskNotFound(String taskId) {
        return new EvictionException(ErrorCode.TaskNotFound, "Task not found: " + taskId);
    }

    public static EvictionException capacityGroupNotFound(String capacityGroupName) {
        return new EvictionException(ErrorCode.CapacityGroupNotFound, "Capacity group not found: " + capacityGroupName);
    }

    public static EvictionException taskAlreadyStopped(Task task) {
        TaskState state = task.getStatus().getState();
        return state == TaskState.Finished
                ? new EvictionException(ErrorCode.TaskAlreadyStopped, "Task already finished: " + task.getId())
                : new EvictionException(ErrorCode.TaskAlreadyStopped, String.format("Task terminating: taskId=%s, state=%s", task.getId(), state));
    }

    public static EvictionException taskNotScheduledYet(Task task) {
        return new EvictionException(ErrorCode.TaskNotScheduledYet, "Task not scheduled yet: " + task.getId());
    }

    public static EvictionException noAvailableGlobalQuota() {
        return new EvictionException(ErrorCode.NoSystemQuota, "No global quota");
    }

    public static EvictionException noAvailableJobQuota(Job<?> job, String reason) {
        return new EvictionException(ErrorCode.NoJobQuota, String.format("No job quota: jobId=%s, reason=%s", job.getId(), reason));
    }
}
