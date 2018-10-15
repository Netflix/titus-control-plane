package com.netflix.titus.supplementary.relocation.model;

import java.util.Objects;

public class TaskRelocationPlan {

    public enum TaskRelocationReason {
        TaskMigration
    }

    private final String taskId;
    private final TaskRelocationReason reason;
    private final String reasonMessage;
    private final long relocationTime;

    public TaskRelocationPlan(String taskId, TaskRelocationReason reason, String reasonMessage, long relocationTime) {
        this.taskId = taskId;
        this.reason = reason;
        this.reasonMessage = reasonMessage;
        this.relocationTime = relocationTime;
    }

    public String getTaskId() {
        return taskId;
    }

    public TaskRelocationReason getReason() {
        return reason;
    }

    public String getReasonMessage() {
        return reasonMessage;
    }

    public long getRelocationTime() {
        return relocationTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TaskRelocationPlan that = (TaskRelocationPlan) o;
        return relocationTime == that.relocationTime &&
                Objects.equals(taskId, that.taskId) &&
                Objects.equals(reason, that.reason) &&
                Objects.equals(reasonMessage, that.reasonMessage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId, reason, reasonMessage, relocationTime);
    }

    public Builder toBuilder() {
        return newBuilder().withTaskId(taskId).withReason(reason).withReasonMessage(reasonMessage).withRelocationTime(relocationTime);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String taskId;
        private TaskRelocationReason reason;
        private String reasonMessage;
        private long relocationTime;

        private Builder() {
        }

        public Builder withTaskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public Builder withReason(TaskRelocationReason reason) {
            this.reason = reason;
            return this;
        }

        public Builder withReasonMessage(String reasonMessage) {
            this.reasonMessage = reasonMessage;
            return this;
        }

        public Builder withRelocationTime(long relocationTime) {
            this.relocationTime = relocationTime;
            return this;
        }

        public TaskRelocationPlan build() {
            return new TaskRelocationPlan(taskId, reason, reasonMessage, relocationTime);
        }
    }
}
