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

package com.netflix.titus.api.jobmanager.model.job;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.netflix.titus.common.util.CollectionsExt;

import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_ATTRIBUTES_CELL;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_ATTRIBUTES_STACK;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_CELL;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_STACK;
import static java.util.Arrays.asList;

public abstract class Task {

    private final String id;
    private final String jobId;
    private final TaskStatus status;
    private final List<TaskStatus> statusHistory;
    private final String originalId;
    private final Optional<String> resubmitOf;
    private final int resubmitNumber;
    private final int systemResubmitNumber;
    private final int evictionResubmitNumber;
    private final List<TwoLevelResource> twoLevelResources;
    private final Map<String, String> taskContext;
    private final Map<String, String> attributes;
    private final Version version;

    protected Task(String id,
                   String jobId,
                   TaskStatus status,
                   List<TaskStatus> statusHistory,
                   String originalId,
                   Optional<String> resubmitOf,
                   int resubmitNumber,
                   int systemResubmitNumber,
                   int evictionResubmitNumber,
                   List<TwoLevelResource> twoLevelResources,
                   Map<String, String> taskContext,
                   Map<String, String> attributes,
                   Version version) {
        this.id = id;
        this.jobId = jobId;
        this.status = status;
        this.statusHistory = CollectionsExt.nullableImmutableCopyOf(statusHistory);
        this.originalId = originalId;
        this.resubmitOf = resubmitOf;
        this.resubmitNumber = resubmitNumber;
        this.systemResubmitNumber = systemResubmitNumber;
        this.evictionResubmitNumber = evictionResubmitNumber;
        this.twoLevelResources = CollectionsExt.nullableImmutableCopyOf(twoLevelResources);
        this.taskContext = CollectionsExt.nullableImmutableCopyOf(taskContext);
        this.attributes = attributes;
        // TODO Remove me. Version is a newly introduced field. If not set explicitly, assign a default value
        this.version = version == null ? Version.undefined() : version;
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

    public int getEvictionResubmitNumber() {
        return evictionResubmitNumber;
    }

    public List<TwoLevelResource> getTwoLevelResources() {
        return twoLevelResources;
    }

    public Map<String, String> getTaskContext() {
        return taskContext;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public Version getVersion() {
        return version;
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
        return resubmitNumber == task.resubmitNumber && systemResubmitNumber == task.systemResubmitNumber && evictionResubmitNumber == task.evictionResubmitNumber && Objects.equals(id, task.id) && Objects.equals(jobId, task.jobId) && Objects.equals(status, task.status) && Objects.equals(statusHistory, task.statusHistory) && Objects.equals(originalId, task.originalId) && Objects.equals(resubmitOf, task.resubmitOf) && Objects.equals(twoLevelResources, task.twoLevelResources) && Objects.equals(taskContext, task.taskContext) && Objects.equals(attributes, task.attributes) && Objects.equals(version, task.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, jobId, status, statusHistory, originalId, resubmitOf, resubmitNumber, systemResubmitNumber, evictionResubmitNumber, twoLevelResources, taskContext, attributes, version);
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
                ", evictionResubmitNumber=" + evictionResubmitNumber +
                ", twoLevelResources=" + twoLevelResources +
                ", taskContext=" + taskContext +
                ", attributes=" + attributes +
                ", version=" + version +
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
        protected int evictionResubmitNumber;
        protected List<TwoLevelResource> twoLevelResources;
        protected Map<String, String> taskContext;
        protected Map<String, String> attributes;
        protected Version version;

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

        public B withEvictionResubmitNumber(int evictionResubmitNumber) {
            this.evictionResubmitNumber = evictionResubmitNumber;
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

        public B addAllToTaskContext(Map<String, String> toAdd) {
            return withTaskContext(CollectionsExt.merge(taskContext, toAdd));
        }

        public B withTaskContext(Map<String, String> taskContext) {
            this.taskContext = taskContext;
            return self();
        }

        public B withAttributes(Map<String, String> attributes) {
            this.attributes = attributes;
            return self();
        }

        public B withCellInfo(Job<?> job) {
            final Map<String, String> jobAttributes = job.getJobDescriptor().getAttributes();
            if (CollectionsExt.containsKeys(jobAttributes, JOB_ATTRIBUTES_CELL)) {
                addToTaskContext(TASK_ATTRIBUTES_CELL, jobAttributes.get(JOB_ATTRIBUTES_CELL));
            }
            if (CollectionsExt.containsKeys(jobAttributes, JOB_ATTRIBUTES_STACK)) {
                addToTaskContext(TASK_ATTRIBUTES_STACK, jobAttributes.get(JOB_ATTRIBUTES_STACK));
            }
            return self();
        }

        public B withCellInfo(Task task) {
            final Map<String, String> oldTaskContext = task.getTaskContext();
            if (CollectionsExt.containsKeys(oldTaskContext, TASK_ATTRIBUTES_CELL)) {
                addToTaskContext(TASK_ATTRIBUTES_CELL, oldTaskContext.get(TASK_ATTRIBUTES_CELL));
            }
            if (CollectionsExt.containsKeys(oldTaskContext, TASK_ATTRIBUTES_STACK)) {
                addToTaskContext(TASK_ATTRIBUTES_STACK, oldTaskContext.get(TASK_ATTRIBUTES_STACK));
            }
            return self();
        }

        public B withVersion(Version version) {
            this.version = version;
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
                    .withEvictionResubmitNumber(evictionResubmitNumber)
                    .withTwoLevelResources(twoLevelResources)
                    .withTaskContext(taskContext)
                    .withAttributes(attributes)
                    .withVersion(version);
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
                    .withEvictionResubmitNumber(task.getEvictionResubmitNumber())
                    .withTwoLevelResources(task.getTwoLevelResources())
                    .withTaskContext(task.getTaskContext())
                    .withAttributes(task.getAttributes())
                    .withVersion(task.getVersion());

        }

        protected B self() {
            return (B) this;
        }
    }
}
