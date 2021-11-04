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

package com.netflix.titus.runtime.connector.jobmanager.snapshot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.tuple.Pair;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;

/**
 * TODO Finished tasks are not handled correctly for batch jobs (they are in active data set until replaced).
 */
public class PCollectionJobSnapshot extends JobSnapshot {

    final PMap<String, CachedJob> cachedJobsById;
    final PMap<String, Job<?>> jobsById;
    final PMap<String, Task> taskById;

    private final boolean autoFixInconsistencies;
    private final boolean archiveMode;
    private final Consumer<String> inconsistentDataListener;
    private final TitusRuntime titusRuntime;

    private final String signature;

    // Deprecated values computed lazily
    private volatile List<Job<?>> allJobs;
    private final Lock allJobsLock = new ReentrantLock();

    private volatile List<Task> allTasks;
    private final Lock allTasksLock = new ReentrantLock();

    private volatile List<Pair<Job<?>, Map<String, Task>>> allJobTaskPairs;
    private final Lock allJobTaskPairsLock = new ReentrantLock();

    public static PCollectionJobSnapshot newInstance(String snapshotId,
                                                     Map<String, Job<?>> jobsById,
                                                     Map<String, Map<String, Task>> tasksByJobId,
                                                     boolean autoFixInconsistencies,
                                                     boolean archiveMode,
                                                     Consumer<String> inconsistentDataListener,
                                                     TitusRuntime titusRuntime) {
        Map<String, Task> taskById = new HashMap<>();
        tasksByJobId.forEach((jobId, tasks) -> taskById.putAll(tasks));

        Map<String, CachedJob> cachedJobsById = new HashMap<>();
        jobsById.forEach((jobId, job) -> {
            Map<String, Task> taskMap = tasksByJobId.get(jobId);
            PMap<String, Task> tasksPMap = CollectionsExt.isNullOrEmpty(taskMap) ? HashTreePMap.empty() : HashTreePMap.from(taskMap);
            cachedJobsById.put(jobId, CachedJob.newInstance(job, tasksPMap, archiveMode, titusRuntime));
        });

        return new PCollectionJobSnapshot(
                snapshotId,
                HashTreePMap.from(cachedJobsById),
                HashTreePMap.from(jobsById),
                HashTreePMap.from(taskById),
                autoFixInconsistencies,
                archiveMode,
                inconsistentDataListener,
                titusRuntime
        );
    }

    private PCollectionJobSnapshot(String snapshotId,
                                   PMap<String, CachedJob> cachedJobsById,
                                   PMap<String, Job<?>> jobsById,
                                   PMap<String, Task> taskById,
                                   boolean autoFixInconsistencies,
                                   boolean archiveMode,
                                   Consumer<String> inconsistentDataListener,
                                   TitusRuntime titusRuntime) {
        super(snapshotId);
        this.cachedJobsById = cachedJobsById;
        this.jobsById = jobsById;
        this.taskById = taskById;
        this.autoFixInconsistencies = autoFixInconsistencies;
        this.archiveMode = archiveMode;
        this.inconsistentDataListener = inconsistentDataListener;
        this.titusRuntime = titusRuntime;
        this.signature = computeSignature();
    }

    public Map<String, Job<?>> getJobMap() {
        return jobsById;
    }

    public List<Job<?>> getJobs() {
        if (allJobs != null) {
            return allJobs;
        }
        allJobsLock.lock();
        try {
            if (allJobs != null) {
                return allJobs;
            }
            this.allJobs = Collections.unmodifiableList(new ArrayList<>(jobsById.values()));
        } finally {
            allJobsLock.unlock();
        }
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
        if (allTasks != null) {
            return allTasks;
        }
        allTasksLock.lock();
        try {
            if (allTasks != null) {
                return allTasks;
            }
            this.allTasks = Collections.unmodifiableList(new ArrayList<>(taskById.values()));
        } finally {
            allTasksLock.unlock();
        }
        return allTasks;
    }

    @Override
    public Map<String, Task> getTasks(String jobId) {
        CachedJob entry = cachedJobsById.get(jobId);
        return entry == null ? HashTreePMap.empty() : entry.getTasks();
    }

    @Override
    public List<Pair<Job<?>, Map<String, Task>>> getJobsAndTasks() {
        if (allJobTaskPairs != null) {
            return allJobTaskPairs;
        }
        allJobTaskPairsLock.lock();
        try {
            if (allJobTaskPairs != null) {
                return allJobTaskPairs;
            }

            List<Pair<Job<?>, Map<String, Task>>> acc = new ArrayList<>();
            cachedJobsById.forEach((jobId, entry) -> acc.add(entry.getJobTasksPair()));
            this.allJobTaskPairs = acc;
        } finally {
            allJobTaskPairsLock.unlock();
        }
        return allJobTaskPairs;
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

    @Override
    public Optional<JobSnapshot> updateJob(Job<?> job) {
        Job<?> previous = jobsById.get(job.getId());
        CachedJob cachedJob = cachedJobsById.get(job.getId());

        // First time seen
        if (previous == null) {
            if (!archiveMode && job.getStatus().getState() == JobState.Finished) {
                return Optional.empty();
            }
            return Optional.of(newSnapshot(
                    cachedJobsById.plus(job.getId(), CachedJob.newInstance(job, HashTreePMap.empty(), archiveMode, titusRuntime)),
                    jobsById.plus(job.getId(), job),
                    taskById
            ));
        }

        // Update
        if (!archiveMode && job.getStatus().getState() == JobState.Finished) {
            return cachedJob.removeJob(this, job);
        }
        return cachedJob.updateJob(this, job);
    }

    @Override
    public Optional<JobSnapshot> removeArchivedJob(Job<?> job) {
        CachedJob cachedJob = cachedJobsById.get(job.getId());
        if (cachedJob == null) {
            return Optional.empty();
        }
        return cachedJob.removeJob(this, job);
    }

    @Override
    public Optional<JobSnapshot> updateTask(Task task, boolean moved) {
        String jobId = task.getJobId();
        CachedJob cachedJob = cachedJobsById.get(jobId);
        if (cachedJob == null) { // Inconsistent data
            inconsistentData("job %s not found for task %s", jobId, task.getId());
            return Optional.empty();
        }

        if (!moved) {
            return cachedJob.updateTask(this, task);
        }

        // Task moved so we have to remove it from the previous job
        String jobIdIndexToUpdate = task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_MOVED_FROM_JOB);
        if (jobIdIndexToUpdate == null) {
            inconsistentData("moved task %s has no attribute with the original job id", task.getId());
            return cachedJob.updateTask(this, task);
        }

        CachedJob previousCachedJob = cachedJobsById.get(jobIdIndexToUpdate);
        PCollectionJobSnapshot currentSnapshot = (PCollectionJobSnapshot) previousCachedJob.removeTask(this, task).orElse(null);
        return currentSnapshot == null
                ? cachedJob.updateTask(this, task)
                : Optional.of(cachedJob.updateTask(currentSnapshot, task).orElse(currentSnapshot));
    }

    @Override
    public Optional<JobSnapshot> removeArchivedTask(Task task) {
        String jobId = task.getJobId();
        CachedJob cachedJob = cachedJobsById.get(jobId);
        if (cachedJob == null) { // Inconsistent data
            inconsistentData("job %s not found for task %s", jobId, task.getId());
            return Optional.empty();
        }
        return cachedJob.removeTask(this, task);
    }

    JobSnapshot newSnapshot(PMap<String, CachedJob> cachedJobsById,
                            PMap<String, Job<?>> jobsById,
                            PMap<String, Task> taskById) {
        return new PCollectionJobSnapshot(
                this.snapshotId,
                cachedJobsById,
                jobsById,
                taskById,
                autoFixInconsistencies,
                archiveMode,
                inconsistentDataListener,
                titusRuntime
        );
    }

    private void inconsistentData(String message, Object... args) {
        String formattedMessage = String.format(message, args);
        if (inconsistentDataListener != null) {
            try {
                inconsistentDataListener.accept(formattedMessage);
            } catch (Exception ignore) {
            }
        }
        if (!autoFixInconsistencies) {
            throw new IllegalStateException(formattedMessage);
        }
    }

    @Override
    public String toSummaryString() {
        return signature;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("JobSnapshot{snapshotId=").append(snapshotId).append(", jobs=");
        jobsById.forEach((id, job) -> {
            CachedJob entry = cachedJobsById.get(id);
            int tasksCount = entry == null ? 0 : entry.getTasks().size();
            sb.append(id).append('=').append(tasksCount).append(',');
        });
        sb.setLength(sb.length() - 1);
        return sb.append('}').toString();
    }

    private String computeSignature() {
        return "JobSnapshot{snapshotId=" + snapshotId +
                ", jobs=" + jobsById.size() +
                ", tasks=" + taskById.size() +
                "}";
    }
}
