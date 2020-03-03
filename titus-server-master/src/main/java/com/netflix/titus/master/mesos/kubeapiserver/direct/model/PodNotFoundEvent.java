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

import java.util.Objects;

import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;

public class PodNotFoundEvent extends PodEvent {

    private final Task task;
    private final TaskStatus finalTaskStatus;

    PodNotFoundEvent(Task task, TaskStatus finalTaskStatus) {
        super(new V1Pod().metadata(new V1ObjectMeta().name(task.getId())));
        this.task = task;
        this.finalTaskStatus = finalTaskStatus;
    }

    public Task getTask() {
        return task;
    }

    public TaskStatus getFinalTaskStatus() {
        return finalTaskStatus;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        PodNotFoundEvent that = (PodNotFoundEvent) o;
        return Objects.equals(task, that.task) &&
                Objects.equals(finalTaskStatus, that.finalTaskStatus);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), task, finalTaskStatus);
    }

    @Override
    public String toString() {
        return "PodNotFoundEvent{" +
                "task=" + task +
                ", finalTaskStatus=" + finalTaskStatus +
                '}';
    }
}
