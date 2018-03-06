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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import io.netflix.titus.common.util.CollectionsExt;

import static java.util.Arrays.asList;

/**
 */
public abstract class Task {

    private final String id;
    private final String jobId;
    private final TaskStatus status;
    private final List<TaskStatus> statusHistory;
    private final String originalId;
    private final Optional<String> resubmitOf;
    private final int resubmitNumber;
    private final int systemResubmitNumber;
    private final List<TwoLevelResource> twoLevelResources;
    private final Map<String, String> taskContext;

    protected Task(String id,
                   String jobId,
                   TaskStatus status,
                   List<TaskStatus> statusHistory,
                   String originalId,
                   Optional<String> resubmitOf,
                   int resubmitNumber,
                   int systemResubmitNumber,
                   List<TwoLevelResource> twoLevelResources,
                   Map<String, String> taskContext) {
        this.id = id;
        this.jobId = jobId;
        this.status = status;
        this.statusHistory = CollectionsExt.nullableImmutableCopyOf(statusHistory);
        this.originalId = originalId;
        this.resubmitOf = resubmitOf;
        this.resubmitNumber = resubmitNumber;
        this.systemResubmitNumber = systemResubmitNumber;
        this.twoLevelResources = CollectionsExt.nullableImmutableCopyOf(twoLevelResources);
        this.taskContext = CollectionsExt.nullableImmutableCopyOf(taskContext);
    }

    public String getId() {
        return id;
    }

    public String getJobId() {
        return jobId;
    }

    public TaskStatus getStatus() {
        return status;
    }

    public List<TaskStatus> getStatusHistory() {
        return statusHistory;
    }

    public String getOriginalId() {
        return originalId;
    }

    public Optional<String> getResubmitOf() {
        return resubmitOf;
    }

    public int getResubmitNumber() {
        return resubmitNumber;
    }

    public int getSystemResubmitNumber() {
        return systemResubmitNumber;
    }

    public List<TwoLevelResource> getTwoLevelResources() {
        return twoLevelResources;
    }

    public Map<String, String> getTaskContext() {
        return taskContext;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Task task = (Task) o;
        return resubmitNumber == task.resubmitNumber &&
                systemResubmitNumber == task.systemResubmitNumber &&
                Objects.equals(id, task.id) &&
                Objects.equals(jobId, task.jobId) &&
                Objects.equals(status, task.status) &&
                Objects.equals(statusHistory, task.statusHistory) &&
                Objects.equals(originalId, task.originalId) &&
                Objects.equals(resubmitOf, task.resubmitOf) &&
                Objects.equals(twoLevelResources, task.twoLevelResources) &&
                Objects.equals(taskContext, task.taskContext);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, jobId, status, statusHistory, originalId, resubmitOf, resubmitNumber, systemResubmitNumber, twoLevelResources, taskContext);
    }

    @Override
    public String toString() {
        return "Task{" +
                "id='" + id + '\'' +
                ", jobId='" + jobId + '\'' +
                ", status=" + status +
                ", statusHistory=" + statusHistory +
                ", originalId='" + originalId + '\'' +
                ", resubmitOf=" + resubmitOf +
                ", resubmitNumber=" + resubmitNumber +
                ", systemResubmitNumber=" + systemResubmitNumber +
                ", twoLevelResources=" + twoLevelResources +
                ", taskContext=" + taskContext +
                '}';
    }

    public abstract TaskBuilder<?, ?> toBuilder();

    public static abstract class TaskBuilder<T extends Task, B extends TaskBuilder<T, B>> {
        protected String id;
        protected String jobId;
        protected TaskStatus status;
        protected List<TaskStatus> statusHistory;
        protected String originalId;
        protected String resubmitOf;
        protected int resubmitNumber;
        protected int systemResubmitNumber;
        protected List<TwoLevelResource> twoLevelResources;
        protected Map<String, String> taskContext;

        public B withId(String id) {
            this.id = id;
            return self();
        }

        public B withJobId(String jobId) {
            this.jobId = jobId;
            return self();
        }

        public B withStatus(TaskStatus status) {
            this.status = status;
            return self();
        }

        public B withStatusHistory(List<TaskStatus> statusHistory) {
            this.statusHistory = statusHistory;
            return self();
        }

        public B withStatusHistory(TaskStatus... statusHistory) {
            if (statusHistory.length == 0) {
                return withStatusHistory(Collections.emptyList());
            }
            if (statusHistory.length == 1) {
                return withStatusHistory(Collections.singletonList(statusHistory[0]));
            }
            return withStatusHistory(asList(statusHistory));
        }

        public B withOriginalId(String originalId) {
            this.originalId = originalId;
            return self();
        }

        public B withResubmitOf(String resubmitOf) {
            this.resubmitOf = resubmitOf;
            return self();
        }

        public B withResubmitNumber(int resubmitNumber) {
            this.resubmitNumber = resubmitNumber;
            return self();
        }

        public B withSystemResubmitNumber(int systemResubmitNumber) {
            this.systemResubmitNumber = systemResubmitNumber;
            return self();
        }

        public B withTwoLevelResources(List<TwoLevelResource> twoLevelResources) {
            this.twoLevelResources = twoLevelResources;
            return self();
        }

        public B withTwoLevelResources(TwoLevelResource... twoLevelResources) {
            if (twoLevelResources.length == 0) {
                return withTwoLevelResources(Collections.emptyList());
            }
            if (twoLevelResources.length == 1) {
                return withTwoLevelResources(Collections.singletonList(twoLevelResources[0]));
            }
            return withTwoLevelResources(asList(twoLevelResources));
        }

        public B addToTaskContext(String key, String value) {
            if (taskContext == null) {
                return withTaskContext(Collections.singletonMap(key, value));
            }

            return withTaskContext(CollectionsExt.copyAndAdd(taskContext, key, value));
        }

        public B withTaskContext(Map<String, String> taskContext) {
            this.taskContext = taskContext;
            return self();
        }

        public abstract T build();

        protected B but(TaskBuilder<T, B> newBuilder) {
            return newBuilder.withId(id)
                    .withJobId(jobId)
                    .withStatus(status)
                    .withStatusHistory(statusHistory)
                    .withOriginalId(originalId)
                    .withResubmitOf(resubmitOf)
                    .withResubmitNumber(resubmitNumber)
                    .withSystemResubmitNumber(resubmitNumber)
                    .withTwoLevelResources(twoLevelResources)
                    .withTaskContext(taskContext);
        }

        protected B newBuilder(TaskBuilder<T, B> newBuilder, Task task) {
            return newBuilder
                    .withId(task.getId())
                    .withJobId(task.getJobId())
                    .withStatus(task.getStatus())
                    .withStatusHistory(task.getStatusHistory())
                    .withOriginalId(task.getOriginalId())
                    .withResubmitOf(task.getResubmitOf().orElse(null))
                    .withResubmitNumber(task.getResubmitNumber())
                    .withSystemResubmitNumber(task.getSystemResubmitNumber())
                    .withTwoLevelResources(task.getTwoLevelResources())
                    .withTaskContext(task.getTaskContext());

        }

        protected B self() {
            return (B) this;
        }
    }
}
