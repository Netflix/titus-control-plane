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

package com.netflix.titus.master.mesos.kubeapiserver.direct.model;

import io.kubernetes.client.models.V1Pod;

public abstract class PodEvent {

    protected final String taskId;
    protected final V1Pod pod;

    protected PodEvent(V1Pod pod) {
        this.taskId = pod.getMetadata().getName();
        this.pod = pod;
    }

    public String getTaskId() {
        return taskId;
    }

    public V1Pod getPod() {
        return pod;
    }

    public static PodAddedEvent onAdd(V1Pod pod) {
        return new PodAddedEvent(pod);
    }

    public static PodUpdatedEvent onUpdate(V1Pod oldPod, V1Pod newPod) {
        return new PodUpdatedEvent(oldPod, newPod);
    }

    public static PodDeletedEvent onDelete(V1Pod pod, boolean deletedFinalStateUnknown) {
        return new PodDeletedEvent(pod, deletedFinalStateUnknown);
    }
}
