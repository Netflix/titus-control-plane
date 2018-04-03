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

package com.netflix.titus.master.mesos;

import java.util.Objects;
import java.util.Optional;

import com.netflix.titus.api.jobmanager.model.job.TaskState;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        V3ContainerEvent that = (V3ContainerEvent) o;
        return timestamp == that.timestamp &&
                Objects.equals(taskId, that.taskId) &&
                taskState == that.taskState &&
                Objects.equals(reasonCode, that.reasonCode) &&
                Objects.equals(reasonMessage, that.reasonMessage) &&
                Objects.equals(titusExecutorDetails, that.titusExecutorDetails);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId, taskState, reasonCode, reasonMessage, timestamp, titusExecutorDetails);
    }

    @Override
    public String toString() {
        return "V3ContainerEvent{" +
                "taskId='" + taskId + '\'' +
                ", taskState=" + taskState +
                ", reasonCode='" + reasonCode + '\'' +
                ", reasonMessage='" + reasonMessage + '\'' +
                ", timestamp=" + timestamp +
                ", titusExecutorDetails=" + titusExecutorDetails +
                '}';
    }
}
