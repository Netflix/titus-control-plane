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

package com.netflix.titus.api.relocation.model;

import java.util.Objects;

import com.google.common.base.Preconditions;

public class TaskRelocationStatus {

    /**
     * Status code set when an operation completed successfully.
     */
    public static final String STATUS_CODE_TERMINATED = "terminated";

    /**
     * Status code set when a task eviction was rejected by the eviction service.
     */
    public static final String STATUS_EVICTION_ERROR = "evictionError";

    /**
     * Status code set when a task could not be terminated due to a system error (for example connectivity issue).
     */
    public static final String STATUS_SYSTEM_ERROR = "systemError";

    public enum TaskRelocationState {
        /// Reason codes:
        //  * 'terminated'
        Success,

        /// Reason codes:
        //  * 'noDisruptionBudget'
        Failure
    }

    private final String taskId;
    private final TaskRelocationState state;
    private final String statusCode;
    private final String statusMessage;
    private final TaskRelocationPlan taskRelocationPlan;
    private final long timestamp;

    public TaskRelocationStatus(String taskId, TaskRelocationState state, String statusCode, String statusMessage, TaskRelocationPlan taskRelocationPlan, long timestamp) {
        this.taskId = taskId;
        this.state = state;
        this.statusCode = statusCode;
        this.statusMessage = statusMessage;
        this.taskRelocationPlan = taskRelocationPlan;
        this.timestamp = timestamp;
    }

    public String getTaskId() {
        return taskId;
    }

    public TaskRelocationState getState() {
        return state;
    }

    public String getStatusCode() {
        return statusCode;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public TaskRelocationPlan getTaskRelocationPlan() {
        return taskRelocationPlan;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TaskRelocationStatus that = (TaskRelocationStatus) o;
        return timestamp == that.timestamp &&
                Objects.equals(taskId, that.taskId) &&
                state == that.state &&
                Objects.equals(statusCode, that.statusCode) &&
                Objects.equals(statusMessage, that.statusMessage) &&
                Objects.equals(taskRelocationPlan, that.taskRelocationPlan);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId, state, statusCode, statusMessage, taskRelocationPlan, timestamp);
    }

    @Override
    public String toString() {
        return "TaskRelocationStatus{" +
                "taskId='" + taskId + '\'' +
                ", state=" + state +
                ", statusCode='" + statusCode + '\'' +
                ", statusMessage='" + statusMessage + '\'' +
                ", taskRelocationPlan=" + taskRelocationPlan +
                ", timestamp=" + timestamp +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder().withTaskId(taskId).withState(state).withStatusCode(statusCode).withStatusMessage(statusMessage).withTaskRelocationPlan(taskRelocationPlan);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String taskId;
        private TaskRelocationState state;
        private String statusCode;
        private TaskRelocationPlan taskRelocationPlan;
        private String statusMessage;
        private long timestamp;

        private Builder() {
        }

        public Builder withTaskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public Builder withState(TaskRelocationState state) {
            this.state = state;
            return this;
        }

        public Builder withStatusCode(String statusCode) {
            this.statusCode = statusCode;
            return this;
        }

        public Builder withStatusMessage(String statusMessage) {
            this.statusMessage = statusMessage;
            return this;
        }

        public Builder withTaskRelocationPlan(TaskRelocationPlan taskRelocationPlan) {
            this.taskRelocationPlan = taskRelocationPlan;
            return this;
        }

        public Builder withTimestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public TaskRelocationStatus build() {
            Preconditions.checkNotNull(taskId, "Task id cannot be null");
            Preconditions.checkNotNull(state, "Task state cannot be null");
            Preconditions.checkNotNull(statusCode, "Status code cannot be null");
            Preconditions.checkNotNull(statusMessage, "Status message cannot be null");
            Preconditions.checkNotNull(taskRelocationPlan, "Task relocation plan cannot be null");
            return new TaskRelocationStatus(taskId, state, statusCode, statusMessage, taskRelocationPlan, timestamp);
        }
    }
}
