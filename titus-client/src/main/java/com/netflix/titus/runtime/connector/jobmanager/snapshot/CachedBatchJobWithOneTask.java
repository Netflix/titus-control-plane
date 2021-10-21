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

import com.google.common.base.Preconditions;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;

/**
 * Most of the batch jobs have size 1. This is an optimized implementation of {@link CachedJob} for this particular case.
 */
public class CachedBatchJobWithOneTask extends CachedJob {

    private final Task task;

    protected CachedBatchJobWithOneTask(Job<?> job, Task task, TitusRuntime titusRuntime) {
        super(job, task == null ? HashTreePMap.empty() : HashTreePMap.singleton(task.getId(), task), titusRuntime);
        this.task = task;
    }

    @Override
    public Optional<JobSnapshot> updateJob(PCollectionJobSnapshot snapshot, Job<?> updatedJob) {
        if (updatedJob == job) {
            return Optional.empty();
        }
        CachedBatchJobWithOneTask update = new CachedBatchJobWithOneTask(updatedJob, task, titusRuntime);
        return Optional.ofNullable(snapshot.newSnapshot(
                snapshot.cachedJobsById.plus(job.getId(), update),
                snapshot.jobsById.plus(job.getId(), updatedJob),
                snapshot.taskById
        ));
    }

    @Override
    public Optional<JobSnapshot> updateTask(PCollectionJobSnapshot snapshot, Task updatedTask) {
        if (task == null) {
            CachedBatchJobWithOneTask update = new CachedBatchJobWithOneTask(job, updatedTask, titusRuntime);
            return Optional.of(snapshot.newSnapshot(
                    snapshot.cachedJobsById.plus(job.getId(), update),
                    snapshot.jobsById,
                    snapshot.taskById.plus(updatedTask.getId(), updatedTask)
            ));
        }

        // This task collides with another one
        if (updatedTask.getVersion().getTimestamp() < task.getVersion().getTimestamp()) {
            // It is an earlier version. Ignore it.
            titusRuntime.getCodeInvariants().inconsistent(
                    "Received earlier version of a task: current=%s, received=%s",
                    task.getVersion().getTimestamp(), updatedTask.getVersion().getTimestamp()
            );
            return Optional.empty();
        }

        // Replace the old version.
        CachedBatchJobWithOneTask update = new CachedBatchJobWithOneTask(job, updatedTask, titusRuntime);

        return Optional.of(snapshot.newSnapshot(
                snapshot.cachedJobsById.plus(job.getId(), update),
                snapshot.jobsById,
                snapshot.taskById.minus(task.getId()).plus(updatedTask.getId(), updatedTask)
        ));
    }

    @Override
    public Optional<JobSnapshot> removeJob(PCollectionJobSnapshot snapshot, Job<?> job) {
        return Optional.of(snapshot.newSnapshot(
                snapshot.cachedJobsById.minus(job.getId()),
                snapshot.jobsById.minus(job.getId()),
                snapshot.taskById.minus(task.getId())
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
        Preconditions.checkState(size == 1, "Expected batch job of size 1");

        Task task = null;
        if (tasks.size() == 1) {
            task = CollectionsExt.first(tasks.values());
        } else if (tasks.size() > 1) {
            // We have to find the latest version.
            for (Task next : tasks.values()) {
                if (task == null) {
                    task = next;
                } else if (task.getVersion().getTimestamp() < next.getVersion().getTimestamp()) {
                    task = next;
                }
            }
        }

        return new CachedBatchJobWithOneTask(job, task, titusRuntime);
    }
}
