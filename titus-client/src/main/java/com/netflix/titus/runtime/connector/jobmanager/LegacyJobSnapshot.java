/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.runtime.connector.jobmanager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.tuple.Pair;

import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

/**
 * TODO Handle moved tasks
 * TODO Finished tasks are not handled correctly for batch jobs (they are in active data set until replaced).
 */
public class LegacyJobSnapshot extends JobSnapshot {

    private static final LegacyJobSnapshot EMPTY = new Builder("empty", Collections.emptyMap(), Collections.emptyMap()).build();

    private final Map<String, Job<?>> jobsById;
    private final Map<String, Map<String, Task>> tasksByJobId;
    private final List<Job<?>> allJobs;
    private final List<Task> allTasks;
    private final List<Pair<Job<?>, Map<String, Task>>> allJobsAndTasks;
    private final Map<String, Task> taskById;

    private final String signature;

    public static LegacyJobSnapshot empty() {
        return EMPTY;
    }

    public static LegacyJobSnapshot newInstance(String snapshotId, Map<String, Job<?>> jobsById, Map<String, Map<String, Task>> tasksByJobId) {
        return new Builder(snapshotId, jobsById, tasksByJobId).build();
    }

    public static Builder newBuilder(String snapshotId, Map<String, Job<?>> jobsById, Map<String, Map<String, Task>> tasksByJobId) {
        return new Builder(snapshotId, jobsById, tasksByJobId);
    }

    public static Builder newBuilder(String snapshotId) {
        return new Builder(snapshotId);
    }

    private LegacyJobSnapshot(String snapshotId, Map<String, Job<?>> jobsById, Map<String, Map<String, Task>> tasksByJobId,
                              List<Job<?>> allJobs, List<Task> allTasks, List<Pair<Job<?>, Map<String, Task>>> allJobsAndTasks,
                              Map<String, Task> taskById) {
        super(snapshotId);
        this.jobsById = jobsById;
        this.tasksByJobId = tasksByJobId;
        this.allJobs = allJobs;
        this.allTasks = allTasks;
        this.allJobsAndTasks = allJobsAndTasks;
        this.taskById = taskById;
        this.signature = computeSignature();
    }

    @Override
    public Map<String, Job<?>> getJobMap() {
        return jobsById;
    }

    public List<Job<?>> getJobs() {
        return allJobs;
    }

    public Optional<Job<?>> findJob(String jobId) {
        return Optional.ofNullable(jobsById.get(jobId));
    }

    @Override
    public Map<String, Task> getTaskMap() {
        return taskById;
    }

    public List<Task> getTasks() {
        return allTasks;
    }

    @Override
    public Map<String, Task> getTasks(String jobId) {
        return tasksByJobId.getOrDefault(jobId, Collections.emptyMap());
    }

    @Override
    public List<Pair<Job<?>, Map<String, Task>>> getJobsAndTasks() {
        return allJobsAndTasks;
    }

    public Optional<Pair<Job<?>, Task>> findTaskById(String taskId) {
        Task task = taskById.get(taskId);
        if (task == null) {
            return Optional.empty();
        }
        Job<?> job = jobsById.get(task.getJobId());
        Preconditions.checkState(job != null); // if this happens there is a bug

        return Optional.of(Pair.of(job, task));
    }

    public Optional<LegacyJobSnapshot> updateJob(Job job) {
        Job<?> previous = jobsById.get(job.getId());
        if (previous == null && job.getStatus().getState() == JobState.Finished) {
            return Optional.empty();
        }

        Builder builder = new Builder(this);
        if (job.getStatus().getState() == JobState.Finished) {
            builder.removeJob(job);
        } else {
            builder.addOrUpdateJob(job);
        }
        return Optional.of(builder.build());
    }

    public Optional<JobSnapshot> updateTask(Task task, boolean moved) {
        if (!jobsById.containsKey(task.getJobId())) { // Inconsistent data
            return Optional.empty();
        }

        Task previous = taskById.get(task.getId());
        if (previous == null && task.getStatus().getState() == TaskState.Finished) {
            return Optional.empty();
        }

        Builder builder = new Builder(this);
        if (task.getStatus().getState() == TaskState.Finished) {
            builder.removeTask(task, moved);
        } else {
            builder.addOrUpdateTask(task, moved);
        }
        return Optional.of(builder.build());
    }

    @Override
    public String toSummaryString() {
        return signature;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("JobSnapshot2{snapshotId=").append(snapshotId).append(", jobs=");
        jobsById.forEach((id, job) -> {
            Map<String, Task> tasks = tasksByJobId.get(id);
            int tasksCount = tasks == null ? 0 : tasks.size();
            sb.append(id).append('=').append(tasksCount).append(',');
        });
        sb.setLength(sb.length() - 1);
        return sb.append('}').toString();
    }

    private String computeSignature() {
        return "LegacyJobSnapshot{snapshotId=" + snapshotId +
                ", jobs=" + jobsById.size() +
                ", tasks=" + allTasks.size() +
                "}";
    }

    public static class Builder {
        private final String snapshotId;
        private final Map<String, Job<?>> jobsById;
        private final Map<String, Map<String, Task>> tasksByJobId;

        private Builder(String snapshotId) {
            this.snapshotId = snapshotId;
            this.jobsById = new HashMap<>();
            this.tasksByJobId = new HashMap<>();
        }

        private Builder(LegacyJobSnapshot from) {
            this(from.snapshotId, from.jobsById, from.tasksByJobId);
        }

        private Builder(String snapshotId, Map<String, Job<?>> jobsById, Map<String, Map<String, Task>> tasksByJobId) {
            this.snapshotId = snapshotId;
            this.jobsById = new HashMap<>(jobsById);
            HashMap<String, Map<String, Task>> copy = new HashMap<>();
            tasksByJobId.forEach((jobId, tasks) -> copy.put(jobId, new HashMap<>(tasks)));
            this.tasksByJobId = copy;
        }

        public LegacyJobSnapshot build() {
            List<Task> allTasks = new ArrayList<>();
            Map<String, Map<String, Task>> immutableTasksByJobId = new HashMap<>();
            Map<String, Task> taskById = new HashMap<>();
            this.tasksByJobId.forEach((jobId, tasks) -> {
                allTasks.addAll(tasks.values());
                immutableTasksByJobId.put(jobId, unmodifiableMap(tasks));
                taskById.putAll(tasks);
            });

            return new LegacyJobSnapshot(
                    snapshotId,
                    unmodifiableMap(jobsById),
                    unmodifiableMap(immutableTasksByJobId),
                    unmodifiableList(new ArrayList<>(jobsById.values())),
                    unmodifiableList(allTasks),
                    buildAllJobsAndTasksList(jobsById, this.tasksByJobId),
                    unmodifiableMap(taskById)
            );

        }

        public Builder removeJob(Job<?> job) {
            jobsById.remove(job.getId());
            tasksByJobId.remove(job.getId());
            return this;
        }

        public Builder addOrUpdateJob(Job<?> job) {
            jobsById.put(job.getId(), job);
            return this;
        }

        public Builder removeTask(Task task, boolean movedFromAnotherJob) {
            String jobIdIndexToUpdate = movedFromAnotherJob ?
                    task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_MOVED_FROM_JOB) :
                    task.getJobId();
            Preconditions.checkArgument(StringExt.isNotEmpty(jobIdIndexToUpdate));

            Map<String, Task> tasks = tasksByJobId.get(jobIdIndexToUpdate);
            if (tasks != null) {
                tasks.remove(task.getId());
            }

            return this;
        }

        public Builder addOrUpdateTask(Task task, boolean movedFromAnotherJob) {
            if (movedFromAnotherJob) {
                removeTask(task, true);
            }
            Map<String, Task> jobTasks = tasksByJobId.get(task.getJobId());
            if (jobTasks == null) {
                jobTasks = new HashMap<>();
                tasksByJobId.put(task.getJobId(), jobTasks);
            }
            jobTasks.put(task.getId(), task);

            return this;
        }

        private static List<Pair<Job<?>, Map<String, Task>>> buildAllJobsAndTasksList(Map<String, Job<?>> jobsById,
                                                                                      Map<String, Map<String, Task>> tasksByJobId) {
            List<Pair<Job<?>, Map<String, Task>>> result = new ArrayList<>();

            jobsById.values().forEach(job -> {
                Map<String, Task> tasks = tasksByJobId.get(job.getId());
                if (CollectionsExt.isNullOrEmpty(tasks)) {
                    result.add(Pair.of(job, Collections.emptyMap()));
                } else {
                    result.add(Pair.of(job, unmodifiableMap(tasks)));
                }
            });

            return unmodifiableList(result);
        }

        public Job<?> getJob(String jobId) {
            return jobsById.get(jobId);
        }
    }
}
