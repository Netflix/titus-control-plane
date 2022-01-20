/*
 * Copyright 2022 Netflix, Inc.
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

package com.netflix.titus.runtime.connector.kubernetes.fabric8io.model;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

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
