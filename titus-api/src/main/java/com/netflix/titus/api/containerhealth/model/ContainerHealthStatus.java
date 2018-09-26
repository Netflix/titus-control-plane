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
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
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
}
