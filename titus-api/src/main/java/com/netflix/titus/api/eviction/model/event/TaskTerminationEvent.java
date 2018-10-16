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
