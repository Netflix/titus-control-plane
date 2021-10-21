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

import java.util.Optional;

import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.StringExt;
import org.pcollections.PMap;
import org.pcollections.PVector;
import org.pcollections.TreePVector;

class CachedBatchJob extends CachedJob {

    private final PVector<Task> tasksByIndex;

    CachedBatchJob(Job<?> job, PMap<String, Task> tasks, PVector<Task> tasksByIndex, TitusRuntime titusRuntime) {
        super(job, tasks, titusRuntime);
        this.tasksByIndex = tasksByIndex;
    }

    @Override
    public Optional<JobSnapshot> updateJob(PCollectionJobSnapshot snapshot, Job<?> updatedJob) {
        if (updatedJob == job) {
            return Optional.empty();
        }
        CachedBatchJob update = new CachedBatchJob(updatedJob, tasks, tasksByIndex, titusRuntime);
        return Optional.ofNullable(snapshot.newSnapshot(
                snapshot.cachedJobsById.plus(job.getId(), update),
                snapshot.jobsById.plus(job.getId(), updatedJob),
                snapshot.taskById
        ));
    }

    @Override
    public Optional<JobSnapshot> updateTask(PCollectionJobSnapshot snapshot, Task task) {
        int index = indexOf(task, tasksByIndex.size(), titusRuntime);
        if (index < 0) {
            return Optional.empty();
        }
        Task current = tasksByIndex.get(index);
        if (current == null) {
            CachedBatchJob update = new CachedBatchJob(
                    job,
                    tasks.plus(task.getId(), task),
                    tasksByIndex.with(index, task),
                    titusRuntime
            );
            return Optional.ofNullable(snapshot.newSnapshot(
                    snapshot.cachedJobsById.plus(job.getId(), update),
                    snapshot.jobsById,
                    snapshot.taskById.plus(task.getId(), task)
            ));
        }

        // This task collides with another one
        if (task.getVersion().getTimestamp() < current.getVersion().getTimestamp()) {
            // It is an earlier version. Ignore it.
            titusRuntime.getCodeInvariants().inconsistent(
                    "Received earlier version of a task: current=%s, received=%s",
                    current.getVersion().getTimestamp(), task.getVersion().getTimestamp()
            );
            return Optional.empty();
        }

        // Replace the old version.
        CachedBatchJob update = new CachedBatchJob(
                job,
                tasks.minus(current.getId()).plus(task.getId(), task),
                tasksByIndex.with(index, task),
                titusRuntime
        );
        return Optional.of(snapshot.newSnapshot(
                snapshot.cachedJobsById.plus(job.getId(), update),
                snapshot.jobsById,
                snapshot.taskById.minus(current.getId()).plus(task.getId(), task)
        ));
    }

    @Override
    public Optional<JobSnapshot> removeJob(PCollectionJobSnapshot snapshot, Job<?> job) {
        return Optional.of(snapshot.newSnapshot(
                snapshot.cachedJobsById.minus(job.getId()),
                snapshot.jobsById.minus(job.getId()),
                snapshot.taskById.minusAll(tasks.keySet())
        ));
    }

    /**
     * We never remove finished batch tasks from a job unless when they are replaced with another one. The latter
     * case is handled by {@link #updateTask(PCollectionJobSnapshot, Task)}.
     */
    @Override
    public Optional<JobSnapshot> removeTask(PCollectionJobSnapshot snapshot, Task task) {
        return Optional.empty();
    }

    public static CachedJob newBatchInstance(Job<BatchJobExt> job, PMap<String, Task> tasks, TitusRuntime titusRuntime) {
        int size = job.getJobDescriptor().getExtensions().getSize();
        PVector<Task> tasksByIndex = TreePVector.empty();
        for (int i = 0; i < size; i++) {
            tasksByIndex = tasksByIndex.plus(null);
        }
        for (Task task : tasks.values()) {
            int index = indexOf(task, size, titusRuntime);
            if (index >= 0) {
                tasksByIndex = tasksByIndex.with(index, task);
            }
        }
        return new CachedBatchJob(job, tasks, tasksByIndex, titusRuntime);
    }

    static int indexOf(Task task, int jobSize, TitusRuntime titusRuntime) {
        String indexStr = task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_TASK_INDEX);
        if (StringExt.isEmpty(indexStr)) {
            titusRuntime.getCodeInvariants().inconsistent(
                    "Batch task without index. Dropped from cache: taskId=%s", task.getId()
            );
            return -1;
        }
        int index;
        try {
            index = Integer.parseInt(indexStr);
        } catch (Exception e) {
            titusRuntime.getCodeInvariants().inconsistent(
                    "Batch task with invalid index. Dropped from cache: taskId=%s, index=%s", task.getId(), indexStr
            );
            return -1;
        }

        if (index < 0 || index >= jobSize) {
            titusRuntime.getCodeInvariants().inconsistent(
                    "Batch task with invalid index. Dropped from cache: taskId=%s, index=%s", task.getId(), indexStr
            );
            return -1;
        }

        return index;
    }
}
