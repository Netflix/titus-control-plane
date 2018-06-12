package com.netflix.titus.api.eviction.model.event;

import java.util.Objects;

public class TaskTerminationEvent extends EvictionEvent {

    private final String taskId;
    private final boolean approved;

    public TaskTerminationEvent(String taskId, boolean approved) {
        this.taskId = taskId;
        this.approved = approved;
    }

    public String getTaskId() {
        return taskId;
    }

    public boolean isApproved() {
        return approved;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TaskTerminationEvent that = (TaskTerminationEvent) o;
        return approved == that.approved &&
                Objects.equals(taskId, that.taskId);
    }

    @Override
    public int hashCode() {

        return Objects.hash(taskId, approved);
    }

    @Override
    public String toString() {
        return "TaskTerminationEvent{" +
                "taskId='" + taskId + '\'' +
                ", approved=" + approved +
                "} " + super.toString();
    }
}
