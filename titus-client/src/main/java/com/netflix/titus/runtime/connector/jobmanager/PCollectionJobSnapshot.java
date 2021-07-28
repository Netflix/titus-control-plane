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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.tuple.Pair;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;
import org.pcollections.PSequence;
import org.pcollections.TreePVector;

public class PCollectionJobSnapshot extends JobSnapshot {

    private static final JobSnapshot EMPTY = newInstance("empty", HashTreePMap.empty(), Collections.emptyMap());

    private final PMap<String, Job<?>> jobsById;
    private final PMap<String, PSequence<Task>> tasksByJobId;
    private final PMap<String, Task> taskById;

    private final String signature;

    // Deprecated values computed lazily
    private volatile List<Job<?>> allJobs;
    private final Lock allJobsLock = new ReentrantLock();

    private volatile List<Task> allTasks;
    private final Lock allTasksLock = new ReentrantLock();

    private volatile List<Pair<Job<?>, List<Task>>> allJobTaskPairs;
    private final Lock allJobTaskPairsLock = new ReentrantLock();

    public static JobSnapshot empty() {
        return EMPTY;
    }

    public static JobSnapshot newInstance(String snapshotId, Map<String, Job<?>> jobsById, Map<String, List<Task>> tasksByJobId) {
        Map<String, PSequence<Task>> mTasksByJobId = new HashMap<>();
        Map<String, Task> taskById = new HashMap<>();
        tasksByJobId.forEach((jobId, tasks) -> {
            TreePVector<Task> pTasks = TreePVector.from(tasks);
            mTasksByJobId.put(jobId, pTasks);
            tasks.forEach(task -> taskById.put(task.getId(), task));
        });

        return new PCollectionJobSnapshot(
                snapshotId,
                HashTreePMap.from(jobsById),
                HashTreePMap.from(mTasksByJobId),
                HashTreePMap.from(taskById)
        );
    }

    private PCollectionJobSnapshot(String snapshotId, PMap<String, Job<?>> jobsById, PMap<String, PSequence<Task>> tasksByJobId,
                                   PMap<String, Task> taskById) {
        super(snapshotId);
        this.jobsById = jobsById;
        this.tasksByJobId = tasksByJobId;
        this.taskById = taskById;
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
            List<Job<?>> acc = new ArrayList<>();
            jobsById.forEach((jobId, job) -> acc.add(job));
            this.allJobs = Collections.unmodifiableList(acc);
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
            List<Task> acc = new ArrayList<>();
            taskById.forEach((taskId, task) -> acc.add(task));
            this.allTasks = acc;
        } finally {
            allTasksLock.unlock();
        }
        return allTasks;
    }

    public List<Task> getTasks(String jobId) {
        PSequence<Task> tasks = tasksByJobId.get(jobId);
        if (tasks == null) {
            return TreePVector.empty();
        }
        return tasks;
    }

    public List<Pair<Job<?>, List<Task>>> getJobsAndTasks() {
        if (allJobTaskPairs != null) {
            return allJobTaskPairs;
        }
        allJobTaskPairsLock.lock();
        try {
            List<Pair<Job<?>, List<Task>>> acc = new ArrayList<>();
            jobsById.forEach((jobId, job) -> {
                PSequence<Task> tasks = tasksByJobId.getOrDefault(jobId, TreePVector.empty());
                acc.add(Pair.of(job, tasks));
            });
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

    public Optional<JobSnapshot> updateJob(Job<?> job) {
        Job<?> previous = jobsById.get(job.getId());
        if (previous == null && job.getStatus().getState() == JobState.Finished) {
            return Optional.empty();
        }

        if (job.getStatus().getState() == JobState.Finished) {
            return Optional.of(removeJob(job));
        } else if (previous == null) {
            return Optional.of(addNewJob(job));
        }
        return Optional.of(updateExistingJob(job));
    }

    public Optional<JobSnapshot> updateTask(Task task, boolean moved) {
        if (!jobsById.containsKey(task.getJobId())) { // Inconsistent data
            return Optional.empty();
        }

        Task previous = taskById.get(task.getId());
        if (previous == null && task.getStatus().getState() == TaskState.Finished) {
            return Optional.empty();
        }

        if (task.getStatus().getState() == TaskState.Finished) {
            return Optional.of(removeTask(task, moved));
        } else if (previous == null) {
            return Optional.of(addNewTask(task));
        }
        return Optional.of(updateExistingTask(task, moved));
    }

    private JobSnapshot addNewJob(Job<?> job) {
        return new PCollectionJobSnapshot(
                this.snapshotId,
                jobsById.plus(job.getId(), job),
                tasksByJobId.plus(job.getId(), TreePVector.empty()),
                taskById
        );
    }

    private JobSnapshot updateExistingJob(Job<?> job) {
        return new PCollectionJobSnapshot(
                this.snapshotId,
                jobsById.plus(job.getId(), job),
                tasksByJobId,
                taskById
        );
    }

    private JobSnapshot removeJob(Job<?> job) {
        PMap<String, Task> newTaskById = taskById;
        for (Task task : taskById.values()) {
            if (task.getJobId().equals(job.getId())) {
                newTaskById = newTaskById.minus(task.getId());
            }
        }

        return new PCollectionJobSnapshot(
                this.snapshotId,
                jobsById.minus(job.getId()),
                tasksByJobId.minus(job.getId()),
                newTaskById
        );
    }

    private JobSnapshot addNewTask(Task task) {
        String jobId = task.getJobId();
        PMap<String, PSequence<Task>> newTasksByJobId = tasksByJobId.plus(jobId, tasksByJobId.get(jobId).plus(task));

        return new PCollectionJobSnapshot(
                this.snapshotId,
                jobsById,
                newTasksByJobId,
                taskById.plus(task.getId(), task)
        );
    }

    private JobSnapshot updateExistingTask(Task task, boolean moved) {
        if (moved) {
            PCollectionJobSnapshot snapshot = (PCollectionJobSnapshot) removeTask(task, true);
            return snapshot.addNewTask(task);
        }

        String jobId = task.getJobId();

        // tasksByJobId
        PSequence<Task> newTasks = replaceTask(task, tasksByJobId.get(jobId));
        Preconditions.checkNotNull(newTasks, "Inconsistent job snapshot. Task %s not found in tasksByJobId collection", task.getId());
        PMap<String, PSequence<Task>> newTasksByJobId = tasksByJobId.plus(jobId, newTasks);

        return new PCollectionJobSnapshot(
                this.snapshotId,
                jobsById,
                newTasksByJobId,
                taskById.plus(task.getId(), task)
        );
    }

    private JobSnapshot removeTask(Task task, boolean moved) {
        String jobIdIndexToUpdate = moved ?
                task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_MOVED_FROM_JOB) :
                task.getJobId();
        Preconditions.checkArgument(StringExt.isNotEmpty(jobIdIndexToUpdate));

        // tasksByJobId
        PSequence<Task> newTasks = removeTask(task, tasksByJobId.get(jobIdIndexToUpdate));
        Preconditions.checkNotNull(newTasks, "Inconsistent job snapshot. Task %s not found in tasksByJobId collection", task.getId());
        PMap<String, PSequence<Task>> newTasksByJobId = tasksByJobId.plus(jobIdIndexToUpdate, newTasks);

        return new PCollectionJobSnapshot(
                this.snapshotId,
                jobsById,
                newTasksByJobId,
                taskById.minus(task.getId())
        );
    }

    private PSequence<Task> replaceTask(Task task, PSequence<Task> tasks) {
        for (int i = 0; i < tasks.size(); i++) {
            Task next = tasks.get(i);
            if (next.getId().equals(task.getId())) {
                return tasks.with(i, task);
            }
        }
        return null;
    }

    private PSequence<Task> removeTask(Task task, PSequence<Task> tasks) {
        for (int i = 0; i < tasks.size(); i++) {
            Task next = tasks.get(i);
            if (next.getId().equals(task.getId())) {
                return tasks.minus(i);
            }
        }
        return null;
    }

    @Override
    public String toSummaryString() {
        return signature;
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

    private String computeSignature() {
        return "JobSnapshot{snapshotId=" + snapshotId +
                ", jobs=" + jobsById.size() +
                ", tasks=" + taskById.size() +
                "}";
    }
}
