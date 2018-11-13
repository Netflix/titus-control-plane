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

package com.netflix.titus.api.eviction.model.event;

import java.util.Objects;
import java.util.Optional;

public class TaskTerminationEvent extends EvictionEvent {

    private final String taskId;
    private final String reason;
    private final boolean approved;
    private final Optional<Throwable> error;

    public TaskTerminationEvent(String taskId, String reason) {
        this.taskId = taskId;
        this.reason = reason;
        this.approved = true;
        this.error = Optional.empty();
    }

    public TaskTerminationEvent(String taskId, String reason, Throwable error) {
        this.taskId = taskId;
        this.reason = reason;
        this.approved = false;
        this.error = Optional.of(error);
    }

    public String getTaskId() {
        return taskId;
    }

    public String getReason() {
        return reason;
    }

    public boolean isApproved() {
        return approved;
    }

    public Optional<Throwable> getError() {
        return error;
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
                Objects.equals(taskId, that.taskId) &&
                Objects.equals(reason, that.reason) &&
                Objects.equals(error, that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId, reason, approved, error);
    }

    @Override
    public String toString() {
        return "TaskTerminationEvent{" +
                "taskId='" + taskId + '\'' +
                ", reason='" + reason + '\'' +
                ", approved=" + approved +
                ", error=" + error +
                '}';
    }
}
