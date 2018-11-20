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

import com.google.common.base.Preconditions;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.tuple.Pair;

import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

/**
 * TODO Handle moved tasks
 * TODO Finished tasks are not handled correctly for batch jobs (they are in active data set until replaced).
 */
public class JobSnapshot {

    private static final JobSnapshot EMPTY = new JobSnapshot(Collections.emptyMap(), Collections.emptyMap());

    private final Map<String, Job<?>> jobsById;
    private final Map<String, List<Task>> tasksByJobId;
    private final List<Job<?>> allJobs;
    private final List<Task> allTasks;
    private final List<Pair<Job<?>, List<Task>>> allJobsAndTasks;
    private final Map<String, Task> taskById;

    public JobSnapshot(Map<String, Job<?>> jobsById, Map<String, List<Task>> tasksByJobId) {
        this.jobsById = jobsById;

        Map<String, List<Task>> immutableTasksByJobId = new HashMap<>();
        tasksByJobId.forEach((jobId, tasks) -> immutableTasksByJobId.put(jobId, unmodifiableList(tasks)));
        this.tasksByJobId = unmodifiableMap(immutableTasksByJobId);

        this.allJobs = Collections.unmodifiableList(new ArrayList<>(jobsById.values()));

        this.allJobsAndTasks = buildAllJobsAndTasksList(jobsById, tasksByJobId);

        List<Task> allTasks = new ArrayList<>();
        tasksByJobId.values().forEach(allTasks::addAll);
        this.allTasks = Collections.unmodifiableList(allTasks);

        Map<String, Task> taskById = new HashMap<>();
        tasksByJobId.values().forEach(tasks -> tasks.forEach(task -> taskById.put(task.getId(), task)));
        this.taskById = taskById;
    }

    private JobSnapshot(JobSnapshot previousCache, Job<?> updatedJob) {
        Job<?> previousJob = previousCache.jobsById.get(updatedJob.getId());

        // We check this condition in the updateJob below.
        Preconditions.checkArgument(previousJob != null || updatedJob.getStatus().getState() != JobState.Finished);

        if (updatedJob.getStatus().getState() == JobState.Finished) {
            // Remove the job and all its tasks.
            this.jobsById = CollectionsExt.copyAndRemove(previousCache.jobsById, updatedJob.getId());
            this.tasksByJobId = CollectionsExt.copyAndRemove(previousCache.tasksByJobId, updatedJob.getId());

            List<Job<?>> allJobs = new ArrayList<>();
            previousCache.allJobs.forEach(job -> {
                if (!job.getId().equals(updatedJob.getId())) {
                    allJobs.add(job);
                }
            });
            this.allJobs = unmodifiableList(allJobs);

            List<Task> allTasks = new ArrayList<>();
            previousCache.allTasks.forEach(task -> {
                if (!task.getJobId().equals(updatedJob.getId())) {
                    allTasks.add(task);
                }
            });
            this.allTasks = unmodifiableList(allTasks);

            List<Pair<Job<?>, List<Task>>> allJobsAndTasks = new ArrayList<>();
            previousCache.allJobsAndTasks.forEach(pair -> {
                if (!pair.getLeft().getId().equals(updatedJob.getId())) {
                    allJobsAndTasks.add(pair);
                }
            });
            this.allJobsAndTasks = unmodifiableList(allJobsAndTasks);

            Map<String, Task> taskById = new HashMap<>();
            previousCache.taskById.values().forEach(task -> {
                if (!task.getJobId().equals(updatedJob.getId())) {
                    taskById.put(task.getId(), task);
                }
            });
            this.taskById = unmodifiableMap(taskById);
        } else {
            this.jobsById = unmodifiableMap(CollectionsExt.copyAndAdd(previousCache.jobsById, updatedJob.getId(), updatedJob));
            this.tasksByJobId = previousCache.tasksByJobId;
            this.allTasks = previousCache.allTasks;
            this.taskById = previousCache.taskById;

            if (previousJob == null) {
                this.allJobs = CollectionsExt.copyAndAdd(previousCache.allJobs, updatedJob);
                this.allJobsAndTasks = CollectionsExt.copyAndAdd(previousCache.allJobsAndTasks, Pair.of(updatedJob, Collections.emptyList()));
            } else {
                List<Job<?>> allJobs = new ArrayList<>();
                previousCache.allJobs.forEach(job -> allJobs.add(job.getId().equals(updatedJob.getId()) ? updatedJob : job));
                this.allJobs = allJobs;

                List<Pair<Job<?>, List<Task>>> allJobsAndTasks = new ArrayList<>();
                previousCache.allJobsAndTasks.forEach(pair -> allJobsAndTasks.add(
                        pair.getLeft().getId().equals(updatedJob.getId()) ? Pair.of(updatedJob, pair.getRight()) : pair
                ));
                this.allJobsAndTasks = allJobsAndTasks;
            }
        }
    }

    private JobSnapshot(JobSnapshot previousCache, Task updatedTask) {
        Task previousTask = previousCache.taskById.get(updatedTask.getId());

        // We check these conditions in the updateTask below.
        Preconditions.checkArgument(previousCache.jobsById.containsKey(updatedTask.getJobId()));
        Preconditions.checkArgument(previousTask != null || updatedTask.getStatus().getState() != TaskState.Finished);

        this.jobsById = previousCache.jobsById;
        this.allJobs = previousCache.allJobs;

        if (updatedTask.getStatus().getState() == TaskState.Finished) {
            List<Task> tasks = removeTask(previousCache.tasksByJobId.get(updatedTask.getJobId()), updatedTask);
            this.tasksByJobId = CollectionsExt.copyAndAdd(previousCache.tasksByJobId, updatedTask.getJobId(), tasks);

            this.allTasks = removeTask(previousCache.allTasks, updatedTask);

            List<Pair<Job<?>, List<Task>>> allJobsAndTasks = new ArrayList<>();
            previousCache.allJobsAndTasks.forEach(pair -> {
                if (pair.getLeft().getId().equals(updatedTask.getJobId())) {
                    allJobsAndTasks.add(Pair.of(pair.getLeft(), removeTask(pair.getRight(), updatedTask)));
                } else {
                    allJobsAndTasks.add(pair);
                }
            });
            this.allJobsAndTasks = unmodifiableList(allJobsAndTasks);
            this.taskById = unmodifiableMap(CollectionsExt.copyAndRemove(previousCache.taskById, updatedTask.getId()));
        } else {
            List<Task> tasks = updateTask(previousCache.tasksByJobId.get(updatedTask.getJobId()), updatedTask);
            this.tasksByJobId = CollectionsExt.copyAndAdd(previousCache.tasksByJobId, updatedTask.getJobId(), tasks);

            this.allTasks = updateTask(previousCache.allTasks, updatedTask);

            List<Pair<Job<?>, List<Task>>> allJobsAndTasks = new ArrayList<>();
            previousCache.allJobsAndTasks.forEach(pair -> {
                if (pair.getLeft().getId().equals(updatedTask.getJobId())) {
                    allJobsAndTasks.add(Pair.of(pair.getLeft(), updateTask(pair.getRight(), updatedTask)));
                } else {
                    allJobsAndTasks.add(pair);
                }
            });
            this.allJobsAndTasks = unmodifiableList(allJobsAndTasks);
            this.taskById = unmodifiableMap(CollectionsExt.copyAndAdd(previousCache.taskById, updatedTask.getId(), updatedTask));
        }
    }

    public List<Job<?>> getJobs() {
        return allJobs;
    }

    public Optional<Job<?>> findJob(String jobId) {
        Job<?> job = jobsById.get(jobId);
        return job == null ? Optional.empty() : Optional.of(job);
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
        Job<?> job = jobsById.get(task.getId());
        // If this happens, we have a bug in the code.
        if (job == null) {
            return Optional.empty();
        }
        return Optional.of(Pair.of(job, task));
    }

    public Optional<JobSnapshot> updateJob(Job job) {
        Job<?> previous = jobsById.get(job.getId());
        if (previous == null && job.getStatus().getState() == JobState.Finished) {
            return Optional.empty();
        }
        return Optional.of(new JobSnapshot(this, job));
    }

    public Optional<JobSnapshot> updateTask(Task task) {
        if (!jobsById.containsKey(task.getJobId())) { // Inconsistent data
            return Optional.empty();
        }

        Task previous = taskById.get(task.getId());
        if (previous == null && task.getStatus().getState() == TaskState.Finished) {
            return Optional.empty();
        }

        return Optional.of(new JobSnapshot(this, task));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("JobSnapshot{jobs=");
        jobsById.forEach((id, job) -> {
            List<Task> tasks = tasksByJobId.get(id);
            int tasksCount = tasks == null ? 0 : tasks.size();
            sb.append(id).append('=').append(tasksCount).append(',');
        });
        sb.setLength(sb.length() - 1);
        return sb.append('}').toString();
    }

    public static JobSnapshot empty() {
        return EMPTY;
    }

    private List<Pair<Job<?>, List<Task>>> buildAllJobsAndTasksList(Map<String, Job<?>> jobsById, Map<String, List<Task>> tasksByJobId) {
        List<Pair<Job<?>, List<Task>>> result = new ArrayList<>();

        jobsById.values().forEach(job -> {
            List<Task> tasks = tasksByJobId.get(job.getId());
            if (CollectionsExt.isNullOrEmpty(tasks)) {
                result.add(Pair.of(job, Collections.emptyList()));
            } else {
                result.add(Pair.of(job, Collections.unmodifiableList(tasks)));
            }
        });

        return Collections.unmodifiableList(result);
    }

    private List<Task> updateTask(List<Task> tasks, Task taskToUpdate) {
        if (CollectionsExt.isNullOrEmpty(tasks)) {
            return Collections.singletonList(taskToUpdate);
        }

        List<Task> result = new ArrayList<>();
        tasks.forEach(task -> {
            if (!task.getId().equals(taskToUpdate.getId())) {
                result.add(task);
            }
        });
        result.add(taskToUpdate);

        return unmodifiableList(result);
    }

    private List<Task> removeTask(List<Task> tasks, Task taskToRemove) {
        List<Task> result = new ArrayList<>();
        tasks.forEach(task -> {
            if (!task.getId().equals(taskToRemove.getId())) {
                result.add(task);
            }
        });
        return unmodifiableList(result);
    }
}
