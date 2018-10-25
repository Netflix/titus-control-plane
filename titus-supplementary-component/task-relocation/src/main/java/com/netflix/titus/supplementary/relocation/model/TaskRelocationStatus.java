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

package com.netflix.titus.supplementary.relocation.model;

import java.util.Objects;

import com.google.common.base.Preconditions;

public class TaskRelocationStatus {

    /**
     * Reason code set when an operation completed successfully.
     */
    public static final String REASON_CODE_TERMINATED = "terminated";

    /**
     * Reason code set when a task eviction was rejected by the eviction service.
     */
    public static final String REASON_EVICTION_ERROR = "evictionError";

    /**
     * Reason code set when a task could not be terminated due to a system error (for example connectivity issue).
     */
    public static final String REASON_SYSTEM_ERROR = "systemError";

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
    private final String reasonCode;
    private final String reasonMessage;
    private final TaskRelocationPlan taskRelocationPlan;

    public TaskRelocationStatus(String taskId, TaskRelocationState state, String reasonCode, String reasonMessage, TaskRelocationPlan taskRelocationPlan) {
        this.taskId = taskId;
        this.state = state;
        this.reasonCode = reasonCode;
        this.reasonMessage = reasonMessage;
        this.taskRelocationPlan = taskRelocationPlan;
    }

    public String getTaskId() {
        return taskId;
    }

    public TaskRelocationState getState() {
        return state;
    }

    public String getReasonCode() {
        return reasonCode;
    }

    public String getReasonMessage() {
        return reasonMessage;
    }

    public TaskRelocationPlan getTaskRelocationPlan() {
        return taskRelocationPlan;
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
        return Objects.equals(taskId, that.taskId) &&
                state == that.state &&
                Objects.equals(reasonCode, that.reasonCode) &&
                Objects.equals(reasonMessage, that.reasonMessage) &&
                Objects.equals(taskRelocationPlan, that.taskRelocationPlan);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId, state, reasonCode, reasonMessage, taskRelocationPlan);
    }

    @Override
    public String toString() {
        return "TaskRelocationStatus{" +
                "taskId='" + taskId + '\'' +
                ", state=" + state +
                ", reasonCode='" + reasonCode + '\'' +
                ", reasonMessage='" + reasonMessage + '\'' +
                ", taskRelocationPlan=" + taskRelocationPlan +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder().withTaskId(taskId).withState(state).withReasonCode(reasonCode).withReasonMessage(reasonMessage).withTaskRelocationPlan(taskRelocationPlan);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String taskId;
        private TaskRelocationState state;
        private String reasonCode;
        private TaskRelocationPlan taskRelocationPlan;
        private String reasonMessage;

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

        public Builder withReasonCode(String reasonCode) {
            this.reasonCode = reasonCode;
            return this;
        }

        public Builder withReasonMessage(String reasonMessage) {
            this.reasonMessage = reasonMessage;
            return this;
        }

        public Builder withTaskRelocationPlan(TaskRelocationPlan taskRelocationPlan) {
            this.taskRelocationPlan = taskRelocationPlan;
            return this;
        }

        public TaskRelocationStatus build() {
            Preconditions.checkNotNull(taskId, "Task id cannot be null");
            Preconditions.checkNotNull(state, "Task state cannot be null");
            Preconditions.checkNotNull(reasonCode, "Reason code cannot be null");
            Preconditions.checkNotNull(reasonMessage, "Reason message cannot be null");
            Preconditions.checkNotNull(taskRelocationPlan, "Task relocation plan cannot be null");
            return new TaskRelocationStatus(taskId, state, reasonCode, reasonMessage, taskRelocationPlan);
        }
    }
}
