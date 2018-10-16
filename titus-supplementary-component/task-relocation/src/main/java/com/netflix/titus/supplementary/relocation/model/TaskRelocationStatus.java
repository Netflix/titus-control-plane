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

public class TaskRelocationStatus {

    public enum TaskRelocationState {
        /// Reason codes:
        //  * 'normal'
        Success,

        /// Reason codes:
        //  * 'noDisruptionBudget'
        Failure
    }

    private final String taskId;
    private final TaskRelocationState state;
    private final String reasonCode;

    public TaskRelocationStatus(String taskId, TaskRelocationState state, String reasonCode) {
        this.taskId = taskId;
        this.state = state;
        this.reasonCode = reasonCode;
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
                Objects.equals(reasonCode, that.reasonCode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId, state, reasonCode);
    }

    @Override
    public String toString() {
        return "TaskRelocationStatus{" +
                "taskId='" + taskId + '\'' +
                ", state=" + state +
                ", reasonCode='" + reasonCode + '\'' +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder().withTaskId(taskId).withState(state).withReasonCode(reasonCode);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String taskId;
        private TaskRelocationState state;
        private String reasonCode;

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

        public TaskRelocationStatus build() {
            return new TaskRelocationStatus(taskId, state, reasonCode);
        }
    }
}
