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

package com.netflix.titus.runtime.connector.jobmanager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

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
public class JobSnapshot {

    private static final JobSnapshot EMPTY = new Builder("empty", Collections.emptyMap(), Collections.emptyMap()).build();

    private final String snapshotId;
    private final Map<String, Job<?>> jobsById;
    private final Map<String, List<Task>> tasksByJobId;
    private final List<Job<?>> allJobs;
    private final List<Task> allTasks;
    private final List<Pair<Job<?>, List<Task>>> allJobsAndTasks;
    private final Map<String, Task> taskById;

    public static JobSnapshot empty() {
        return EMPTY;
    }

    public static JobSnapshot newInstance(String snapshotId, Map<String, Job<?>> jobsById, Map<String, List<Task>> tasksByJobId) {
        return new Builder(snapshotId, jobsById, tasksByJobId).build();
    }

    public static Builder newBuilder(String snapshotId, Map<String, Job<?>> jobsById, Map<String, List<Task>> tasksByJobId) {
        return new Builder(snapshotId, jobsById, tasksByJobId);
    }

    public static Builder newBuilder(String snapshotId) {
        return new Builder(snapshotId);
    }

    private JobSnapshot(String snapshotId, Map<String, Job<?>> jobsById, Map<String, List<Task>> tasksByJobId,
                        List<Job<?>> allJobs, List<Task> allTasks, List<Pair<Job<?>, List<Task>>> allJobsAndTasks,
                        Map<String, Task> taskById) {
        this.snapshotId = snapshotId;
        this.jobsById = jobsById;
        this.tasksByJobId = tasksByJobId;
        this.allJobs = allJobs;
        this.allTasks = allTasks;
        this.allJobsAndTasks = allJobsAndTasks;
        this.taskById = taskById;
    }

    public String getSnapshotId() {
        return snapshotId;
    }

    public List<Job<?>> getJobs() {
        return allJobs;
    }

    public Optional<Job<?>> findJob(String jobId) {
        return Optional.ofNullable(jobsById.get(jobId));
    }

    public List<Task> getTasks() {
        return allTasks;
    }

    public List<Task> getTasks(String jobId) {
        return tasksByJobId.getOrDefault(jobId, Collections.emptyList());
    }

    public List<Pair<Job<?>, List<Task>>> getJobsAndTasks() {
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

    public Optional<JobSnapshot> updateJob(Job job) {
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
    public String toString() {
        StringBuilder sb = new StringBuilder("JobSnapshot{snapshotId=").append(snapshotId).append(", jobs=");
        jobsById.forEach((id, job) -> {
            List<Task> tasks = tasksByJobId.get(id);
            int tasksCount = tasks == null ? 0 : tasks.size();
            sb.append(id).append('=').append(tasksCount).append(',');
        });
        sb.setLength(sb.length() - 1);
        return sb.append('}').toString();
    }

    public static class Builder {
        private final String snapshotId;
        private final Map<String, Job<?>> jobsById;
        private final Map<String, List<Task>> tasksByJobId;

        private Builder(String snapshotId) {
            this.snapshotId = snapshotId;
            this.jobsById = new HashMap<>();
            this.tasksByJobId = new HashMap<>();
        }

        private Builder(JobSnapshot from) {
            this(from.snapshotId, from.jobsById, from.tasksByJobId);
        }

        private Builder(String snapshotId, Map<String, Job<?>> jobsById, Map<String, List<Task>> tasksByJobId) {
            this.snapshotId = snapshotId;
            this.jobsById = new HashMap<>(jobsById);
            HashMap<String, List<Task>> copy = new HashMap<>();
            tasksByJobId.forEach((jobId, tasks) -> copy.put(jobId, new ArrayList<>(tasks)));
            this.tasksByJobId = copy;
        }

        public JobSnapshot build() {
            Map<String, List<Task>> immutableTasksByJobId = new HashMap<>();
            tasksByJobId.forEach((jobId, tasks) -> immutableTasksByJobId.put(jobId, unmodifiableList(tasks)));

            List<Task> allTasks = new ArrayList<>();
            tasksByJobId.values().forEach(allTasks::addAll);

            Map<String, Task> taskById = allTasks.stream().collect(Collectors.toMap(Task::getId, Function.identity()));

            return new JobSnapshot(
                    snapshotId,
                    unmodifiableMap(jobsById),
                    unmodifiableMap(immutableTasksByJobId),
                    unmodifiableList(new ArrayList<>(jobsById.values())),
                    unmodifiableList(allTasks),
                    buildAllJobsAndTasksList(jobsById, tasksByJobId),
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

            if (tasksByJobId.containsKey(jobIdIndexToUpdate)) {
                tasksByJobId.get(jobIdIndexToUpdate).removeIf(t -> t.getId().equals(task.getId()));
            }

            return this;
        }

        public Builder addOrUpdateTask(Task task, boolean movedFromAnotherJob) {
            if (movedFromAnotherJob) {
                removeTask(task, true);
            }
            tasksByJobId.putIfAbsent(task.getJobId(), new ArrayList<>());
            List<Task> jobTasks = tasksByJobId.get(task.getJobId());
            jobTasks.removeIf(t -> t.getId().equals(task.getId()));
            jobTasks.add(task);

            return this;
        }

        private static List<Pair<Job<?>, List<Task>>> buildAllJobsAndTasksList(Map<String, Job<?>> jobsById, Map<String, List<Task>> tasksByJobId) {
            List<Pair<Job<?>, List<Task>>> result = new ArrayList<>();

            jobsById.values().forEach(job -> {
                List<Task> tasks = tasksByJobId.get(job.getId());
                if (CollectionsExt.isNullOrEmpty(tasks)) {
                    result.add(Pair.of(job, Collections.emptyList()));
                } else {
                    result.add(Pair.of(job, unmodifiableList(tasks)));
                }
            });

            return unmodifiableList(result);
        }

        public Job<?> getJob(String jobId) {
            return jobsById.get(jobId);
        }
    }
}
