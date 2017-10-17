/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.api.model.event;

/**
 * Task state change event.
 */
public class TaskStateChangeEvent<SOURCE, STATE> extends SchedulingEvent<SOURCE> {

    private final String taskId;
    private final STATE state;

    public TaskStateChangeEvent(String jobId, String taskId, STATE state, long timestamp, SOURCE source) {
        super(jobId, timestamp, source);
        this.taskId = taskId;
        this.state = state;
    }

    public String getTaskId() {
        return taskId;
    }

    public STATE getState() {
        return state;
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

        TaskStateChangeEvent<?, ?> that = (TaskStateChangeEvent<?, ?>) o;

        if (taskId != null ? !taskId.equals(that.taskId) : that.taskId != null) {
            return false;
        }
        return state != null ? state.equals(that.state) : that.state == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (taskId != null ? taskId.hashCode() : 0);
        result = 31 * result + (state != null ? state.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TaskStateChangeEvent{" +
                "jobId='" + getJobId() + '\'' +
                ", taskId='" + taskId + '\'' +
                ", state=" + state +
                ", timestamp=" + getTimestamp() +
                ", sourceType=" + getSource().getClass() +
                '}';
    }
}
