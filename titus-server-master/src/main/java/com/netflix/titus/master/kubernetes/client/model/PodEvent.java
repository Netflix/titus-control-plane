/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.master.kubernetes.client.model;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.master.kubernetes.KubeObjectFormatter;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1Pod;

public abstract class PodEvent {

    private static final AtomicLong SEQUENCE_NUMBER = new AtomicLong();

    protected final String taskId;
    protected final V1Pod pod;
    protected final long sequenceNumber;

    protected PodEvent(V1Pod pod) {
        this.taskId = pod.getMetadata().getName();
        this.pod = pod;
        this.sequenceNumber = SEQUENCE_NUMBER.getAndIncrement();
    }

    public String getTaskId() {
        return taskId;
    }

    public V1Pod getPod() {
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
                ", pod=" + KubeObjectFormatter.formatPodEssentials(pod) +
                '}';
    }

    public static long nextSequence() {
        return SEQUENCE_NUMBER.get();
    }

    public static PodAddedEvent onAdd(V1Pod pod) {
        return new PodAddedEvent(pod);
    }

    public static PodUpdatedEvent onUpdate(V1Pod oldPod, V1Pod newPod, Optional<V1Node> node) {
        return new PodUpdatedEvent(oldPod, newPod, node);
    }

    public static PodDeletedEvent onDelete(V1Pod pod, boolean deletedFinalStateUnknown, Optional<V1Node> node) {
        return new PodDeletedEvent(pod, deletedFinalStateUnknown, node);
    }

    public static PodNotFoundEvent onPodNotFound(Task task, TaskStatus finalTaskStatus) {
        return new PodNotFoundEvent(task, finalTaskStatus);
    }
}
