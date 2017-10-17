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

package io.netflix.titus.testkit.perf.load.job;

import io.netflix.titus.api.endpoint.v2.rest.representation.TaskInfo;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobState;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskState;

public abstract class JobChangeEvent {

    private final String jobId;

    protected JobChangeEvent(String jobId) {
        this.jobId = jobId;
    }

    public String getJobId() {
        return jobId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JobChangeEvent that = (JobChangeEvent) o;

        return jobId != null ? jobId.equals(that.jobId) : that.jobId == null;
    }

    @Override
    public int hashCode() {
        return jobId != null ? jobId.hashCode() : 0;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "jobId='" + jobId + '\'' +
                '}';
    }

    public static class JobSubmitEvent extends JobChangeEvent {
        private JobSubmitEvent(String jobId) {
            super(jobId);
        }
    }

    public static class JobFinishedEvent extends JobChangeEvent {
        private final TitusJobState state;

        private JobFinishedEvent(String jobId, TitusJobState state) {
            super(jobId);
            this.state = state;
        }

        public TitusJobState getState() {
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

            JobFinishedEvent that = (JobFinishedEvent) o;

            return state == that.state;
        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + (state != null ? state.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{" +
                    "jobId='" + getJobId() + '\'' +
                    "state=" + state +
                    '}';
        }
    }

    public static class JobKillRequestedEvent extends JobChangeEvent {
        private JobKillRequestedEvent(String jobId) {
            super(jobId);
        }
    }

    public static class UpdateInstanceCountRequestedEvent extends JobChangeEvent {
        private final int min;
        private final int desired;
        private final int max;

        private UpdateInstanceCountRequestedEvent(String jobId, int min, int desired, int max) {
            super(jobId);
            this.min = min;
            this.desired = desired;
            this.max = max;
        }

        public int getMin() {
            return min;
        }

        public int getDesired() {
            return desired;
        }

        public int getMax() {
            return max;
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

            UpdateInstanceCountRequestedEvent that = (UpdateInstanceCountRequestedEvent) o;

            if (min != that.min) {
                return false;
            }
            if (desired != that.desired) {
                return false;
            }
            return max == that.max;
        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + min;
            result = 31 * result + desired;
            result = 31 * result + max;
            return result;
        }

        @Override
        public String toString() {
            return "UpdateInstanceCountRequested{" +
                    "jobId='" + getJobId() + '\'' +
                    "min=" + min +
                    ", desired=" + desired +
                    ", max=" + max +
                    '}';
        }
    }

    public static class TaskEvent extends JobChangeEvent {

        private final String taskId;

        protected TaskEvent(String jobId, String taskId) {
            super(jobId);
            this.taskId = taskId;
        }

        public String getTaskId() {
            return taskId;
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

            TaskEvent taskEvent = (TaskEvent) o;

            return taskId != null ? taskId.equals(taskEvent.taskId) : taskEvent.taskId == null;
        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + (taskId != null ? taskId.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{" +
                    "jobId='" + getJobId() + '\'' +
                    "taskId='" + taskId + '\'' +
                    '}';
        }
    }

    public static class TaskStateChangeEvent extends TaskEvent {
        private final TitusTaskState taskState;

        protected TaskStateChangeEvent(String jobId, String taskId, TitusTaskState taskState) {
            super(jobId, taskId);
            this.taskState = taskState;
        }

        public TitusTaskState getTaskState() {
            return taskState;
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

            TaskStateChangeEvent that = (TaskStateChangeEvent) o;

            return taskState == that.taskState;
        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + (taskState != null ? taskState.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{" +
                    "jobId='" + getJobId() + '\'' +
                    "taskId='" + getTaskId() + '\'' +
                    ", taskState=" + taskState +
                    '}';
        }
    }

    public static class TaskCreateEvent extends TaskStateChangeEvent {
        protected TaskCreateEvent(String jobId, String taskId, TitusTaskState taskState) {
            super(jobId, taskId, taskState);
        }
    }


    public static class TaskKillRequestedEvent extends TaskEvent {
        protected TaskKillRequestedEvent(String jobId, String taskId) {
            super(jobId, taskId);
        }
    }

    public static class TaskTerminateAndShrinkRequestedEvent extends TaskEvent {
        protected TaskTerminateAndShrinkRequestedEvent(String jobId, String taskId) {
            super(jobId, taskId);
        }
    }

    public static class InconsistentStateEvent extends JobChangeEvent {

        private final int expectedTasks;
        private final int actualTasks;

        protected InconsistentStateEvent(String jobId, int expectedTasks, int actualTasks) {
            super(jobId);
            this.expectedTasks = expectedTasks;
            this.actualTasks = actualTasks;
        }

        public int getExpectedTasks() {
            return expectedTasks;
        }

        public int getActualTasks() {
            return actualTasks;
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

            InconsistentStateEvent that = (InconsistentStateEvent) o;

            if (expectedTasks != that.expectedTasks) {
                return false;
            }
            return actualTasks == that.actualTasks;
        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + expectedTasks;
            result = 31 * result + actualTasks;
            return result;
        }

        @Override
        public String toString() {
            return "InconsistentStateEvent{" +
                    "jobId='" + getJobId() + '\'' +
                    "expectedTasks=" + expectedTasks +
                    ", actualTasks=" + actualTasks +
                    '}';
        }
    }

    public static class ConsistencyRestoreEvent extends JobChangeEvent {

        private final int expectedTasks;

        protected ConsistencyRestoreEvent(String jobId, int expectedTasks) {
            super(jobId);
            this.expectedTasks = expectedTasks;
        }

        public int getExpectedTasks() {
            return expectedTasks;
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

            ConsistencyRestoreEvent that = (ConsistencyRestoreEvent) o;

            return expectedTasks == that.expectedTasks;
        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + expectedTasks;
            return result;
        }

        @Override
        public String toString() {
            return "ConsistencyRestoreEvent{" +
                    "jobId='" + getJobId() + '\'' +
                    "expectedTasks=" + expectedTasks +
                    '}';
        }
    }

    public static JobChangeEvent onJobSubmit(String jobId) {
        return new JobSubmitEvent(jobId);
    }

    public static JobFinishedEvent onJobFinished(String jobId, TitusJobState state) {
        return new JobFinishedEvent(jobId, state);
    }

    public static JobKillRequestedEvent onJobKillRequested(String jobId) {
        return new JobKillRequestedEvent(jobId);
    }

    public static JobChangeEvent onTaskCreate(String jobId, TaskInfo taskInfo) {
        return new TaskCreateEvent(jobId, taskInfo.getId(), taskInfo.getState());
    }

    public static TaskStateChangeEvent onTaskStateChange(String jobId, String taskId, TitusTaskState taskState) {
        return new TaskStateChangeEvent(jobId, taskId, taskState);
    }

    public static TaskKillRequestedEvent onTaskKillRequested(String jobId, String taskId) {
        return new TaskKillRequestedEvent(jobId, taskId);
    }

    public static TaskTerminateAndShrinkRequestedEvent onTaskTerminateAndShrink(String jobId, String taskId) {
        return new TaskTerminateAndShrinkRequestedEvent(jobId, taskId);
    }

    public static UpdateInstanceCountRequestedEvent onUpdateInstanceCountRequest(String jobId, int min, int desired, int max) {
        return new UpdateInstanceCountRequestedEvent(jobId, min, desired, max);
    }

    public static InconsistentStateEvent onInconsistentState(String jobId, int expectedTasks, int actualTasks) {
        return new InconsistentStateEvent(jobId, expectedTasks, actualTasks);
    }

    public static ConsistencyRestoreEvent onConsistencyRestore(String jobId, int expectedTasks) {
        return new ConsistencyRestoreEvent(jobId, expectedTasks);
    }
}
