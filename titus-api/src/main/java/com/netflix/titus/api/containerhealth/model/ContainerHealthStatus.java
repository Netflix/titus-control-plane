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

package com.netflix.titus.api.containerhealth.model;

import java.util.Objects;

import com.google.common.base.Preconditions;

/**
 * {@link ContainerHealthStatus} represents an application health status in a point in time.
 */
public class ContainerHealthStatus {

    private final String taskId;
    private final ContainerHealthState state;
    private final String reason;
    private final long timestamp;

    public ContainerHealthStatus(String taskId, ContainerHealthState state, String reason, long timestamp) {
        this.taskId = taskId;
        this.state = state;
        this.reason = reason;
        this.timestamp = timestamp;
    }

    public String getTaskId() {
        return taskId;
    }

    public ContainerHealthState getState() {
        return state;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getReason() {
        return reason;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ContainerHealthStatus that = (ContainerHealthStatus) o;
        return timestamp == that.timestamp &&
                Objects.equals(taskId, that.taskId) &&
                state == that.state &&
                Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId, state, reason, timestamp);
    }

    @Override
    public String toString() {
        return "ContainerHealthStatus{" +
                "taskId='" + taskId + '\'' +
                ", state=" + state +
                ", reason='" + reason + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    public static ContainerHealthStatus healthy(String taskId, long timestamp) {
        return newStatus(taskId, timestamp, "good", ContainerHealthState.Healthy);
    }

    public static ContainerHealthStatus unhealthy(String taskId, String reason, long timestamp) {
        return newStatus(taskId, timestamp, reason, ContainerHealthState.Unhealthy);
    }

    public static ContainerHealthStatus unknown(String taskId, String reason, long timestamp) {
        return newStatus(taskId, timestamp, reason, ContainerHealthState.Unhealthy);
    }

    public static ContainerHealthStatus terminated(String taskId, long timestamp) {
        return newStatus(taskId, timestamp, "terminated", ContainerHealthState.Terminated);
    }

    private static ContainerHealthStatus newStatus(String taskId, long timestamp, String reason, ContainerHealthState state) {
        return ContainerHealthStatus.newBuilder()
                .withTaskId(taskId)
                .withTimestamp(timestamp)
                .withState(state)
                .withReason(reason)
                .build();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String taskId;
        private ContainerHealthState state;
        private String reason;
        private long timestamp;

        private Builder() {
        }

        public Builder withTaskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public Builder withState(ContainerHealthState state) {
            this.state = state;
            return this;
        }

        public Builder withReason(String reason) {
            this.reason = reason;
            return this;
        }

        public Builder withTimestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder but() {
            return newBuilder().withTaskId(taskId).withState(state).withReason(reason).withTimestamp(timestamp);
        }

        public ContainerHealthStatus build() {
            Preconditions.checkNotNull(taskId, "Task id not set");
            Preconditions.checkNotNull(state, "Container health state not set");
            Preconditions.checkNotNull(reason, "Container health reason not set");

            return new ContainerHealthStatus(taskId, state, reason, timestamp);
        }
    }
}
