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

package io.netflix.titus.api.jobmanager.model.job;

import java.util.Objects;

import io.netflix.titus.common.model.sanitizer.NeverNull;

/**
 */
@NeverNull
public class TaskStatus extends ExecutableStatus<TaskState> {

    public static final String REASON_NORMAL = "normal";

    public static final String REASON_JOB_KILLED = "killed";

    public static final String REASON_TASK_KILLED = "killed";

    public static final String REASON_TASK_LOST = "lost";

    public static final String REASON_SCALED_DOWN = "scaledDown";

    public static final String REASON_STUCK_IN_STATE = "stuckInState";

    public static final String REASON_ERROR = "error";

    public static final String REASON_FAILED = "failed";

    public static final String REASON_UNKNOWN = "unknown";


    public TaskStatus(TaskState taskState, String reasonCode, String reasonMessage, long timestamp) {
        super(taskState, reasonCode, reasonMessage, timestamp);
    }

    public static boolean areEquivalent(TaskStatus first, TaskStatus second) {
        if (first.getState() != second.getState()) {
            return false;
        }
        if (!Objects.equals(first.getReasonCode(), second.getReasonCode())) {
            return false;
        }
        return Objects.equals(first.getReasonMessage(), second.getReasonMessage());
    }

    public Builder toBuilder() {
        return newBuilder(this);
    }

    public static TaskStatus.Builder newBuilder() {
        return new TaskStatus.Builder();
    }

    public static TaskStatus.Builder newBuilder(TaskStatus taskStatus) {
        return new TaskStatus.Builder(taskStatus);
    }

    public static class Builder extends AbstractBuilder<TaskState, Builder, TaskStatus> {
        private Builder() {
        }

        private Builder(TaskStatus status) {
            super(status);
        }

        @Override
        public TaskStatus build() {
            return new TaskStatus(state, reasonCode, toCompleteReasonMessage(), timestamp);
        }
    }
}
