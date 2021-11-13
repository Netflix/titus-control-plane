package com.netflix.titus.gateway.kubernetes;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.Pod;

public abstract class PodEvent {

    private static final AtomicLong SEQUENCE_NUMBER = new AtomicLong();

    protected final String taskId;
    protected final Pod pod;
    protected final long sequenceNumber;

    protected PodEvent(Pod pod) {
        this.taskId = pod.getMetadata().getName();
        this.pod = pod;
        this.sequenceNumber = SEQUENCE_NUMBER.getAndIncrement();
    }

    public String getTaskId() {
        return taskId;
    }

    public Pod getPod() {
        return pod;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PodEvent podEvent = (PodEvent) o;
        return sequenceNumber == podEvent.sequenceNumber && Objects.equals(taskId, podEvent.taskId) && Objects.equals(pod, podEvent.pod);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId, pod, sequenceNumber);
    }

    @Override
    public String toString() {
        return "PodEvent{" +
                "taskId='" + taskId + '\'' +
                ", sequenceNumber=" + sequenceNumber +
                ", pod=" + F8KubeObjectFormatter.formatPodEssentials(pod) +
                '}';
    }

    public static long nextSequence() {
        return SEQUENCE_NUMBER.get();
    }

    public static PodAddedEvent onAdd(Pod pod) {
        return new PodAddedEvent(pod);
    }

    public static PodUpdatedEvent onUpdate(Pod oldPod, Pod newPod, Optional<Node> node) {
        return new PodUpdatedEvent(oldPod, newPod, node);
    }

    public static PodDeletedEvent onDelete(Pod pod, boolean deletedFinalStateUnknown, Optional<Node> node) {
        return new PodDeletedEvent(pod, deletedFinalStateUnknown, node);
    }

    /*public static PodNotFoundEvent onPodNotFound(Task task, TaskStatus finalTaskStatus) {
        return new PodNotFoundEvent(task, finalTaskStatus);
    }*/
}
