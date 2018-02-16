package io.netflix.titus.master.mesos;

import java.util.Optional;

import io.netflix.titus.api.jobmanager.model.job.TaskState;

public class V3ContainerEvent implements ContainerEvent {

    private final String taskId;
    private final TaskState taskState;
    private final String reasonCode;
    private final String reasonMessage;
    private final long timestamp;
    private final Optional<TitusExecutorDetails> titusExecutorDetails;

    public V3ContainerEvent(String taskId,
                            TaskState taskState,
                            String reasonCode,
                            String reasonMessage,
                            long timestamp,
                            Optional<TitusExecutorDetails> titusExecutorDetails) {
        this.taskId = taskId;
        this.taskState = taskState;
        this.reasonCode = reasonCode;
        this.reasonMessage = reasonMessage;
        this.timestamp = timestamp;
        this.titusExecutorDetails = titusExecutorDetails;
    }

    public String getTaskId() {
        return taskId;
    }

    public TaskState getTaskState() {
        return taskState;
    }

    public String getReasonCode() {
        return reasonCode;
    }

    public String getReasonMessage() {
        return reasonMessage;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Optional<TitusExecutorDetails> getTitusExecutorDetails() {
        return titusExecutorDetails;
    }
}
