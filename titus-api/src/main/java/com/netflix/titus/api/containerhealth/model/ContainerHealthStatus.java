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

/**
 * {@link ContainerHealthStatus} represents an application health status in a point in time.
 */
public class ContainerHealthStatus {

    private final String taskId;
    private final ContainerHealthState state;
    private final long timestamp;

    public ContainerHealthStatus(String taskId, ContainerHealthState state, long timestamp) {
        this.taskId = taskId;
        this.state = state;
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
                state == that.state;
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId, state, timestamp);
    }

    @Override
    public String toString() {
        return "ContainerHealthStatus{" +
                "taskId='" + taskId + '\'' +
                ", state=" + state +
                ", timestamp=" + timestamp +
                '}';
    }

    public static ContainerHealthStatus healthy(String taskId, long timestamp) {
        return newStatus(taskId, timestamp, ContainerHealthState.Healthy);
    }

    public static ContainerHealthStatus unhealthy(String taskId, long timestamp) {
        return newStatus(taskId, timestamp, ContainerHealthState.Unhealthy);
    }

    public static ContainerHealthStatus unknown(String taskId, long timestamp) {
        return newStatus(taskId, timestamp, ContainerHealthState.Unhealthy);
    }

    public static ContainerHealthStatus terminated(String taskId, long timestamp) {
        return newStatus(taskId, timestamp, ContainerHealthState.Terminated);
    }

    private static ContainerHealthStatus newStatus(String taskId, long timestamp, ContainerHealthState state) {
        return ContainerHealthStatus.newBuilder()
                .withTaskId(taskId)
                .withTimestamp(timestamp)
                .withState(state)
                .build();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String taskId;
        private ContainerHealthState state;
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

        public Builder withTimestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder but() {
            return newBuilder().withTaskId(taskId).withState(state).withTimestamp(timestamp);
        }

        public ContainerHealthStatus build() {
            return new ContainerHealthStatus(taskId, state, timestamp);
        }
    }
}
