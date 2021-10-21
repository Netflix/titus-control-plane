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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.common.runtime.TitusRuntime;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;

/**
 * TODO {@link CachedServiceJob} stores only non-finished service tasks, as we cannot distinguish now finished/not-replaced
 * from finished/scaled down. To fix this we would have to add task archived event to explicitly remove it from the cache.
 */
class CachedServiceJob extends CachedJob {

    CachedServiceJob(Job<?> job, PMap<String, Task> tasks, TitusRuntime titusRuntime) {
        super(job, tasks, titusRuntime);
    }

    @Override
    public Optional<JobSnapshot> updateJob(PCollectionJobSnapshot snapshot, Job<?> updatedJob) {
        if (updatedJob == job) {
            return Optional.empty();
        }
        CachedServiceJob update = new CachedServiceJob(updatedJob, tasks, titusRuntime);
        return Optional.ofNullable(snapshot.newSnapshot(
                snapshot.cachedJobsById.plus(job.getId(), update),
                snapshot.jobsById.plus(job.getId(), updatedJob),
                snapshot.taskById
        ));
    }

    @Override
    public Optional<JobSnapshot> updateTask(PCollectionJobSnapshot snapshot, Task updatedTask) {
        String taskId = updatedTask.getId();
        Task currentTaskVersion = tasks.get(taskId);

        // TODO See above.
        if (updatedTask.getStatus().getState() == TaskState.Finished) {
            if (currentTaskVersion == null) {
                return Optional.empty();
            }
            return removeTask(snapshot, updatedTask);
        }

        if (currentTaskVersion == null) {
            CachedServiceJob update = new CachedServiceJob(job, tasks.plus(taskId, updatedTask), titusRuntime);
            return Optional.ofNullable(snapshot.newSnapshot(
                    snapshot.cachedJobsById.plus(job.getId(), update),
                    snapshot.jobsById,
                    snapshot.taskById.plus(taskId, updatedTask)
            ));
        }

        // This task collides with another one
        if (updatedTask.getVersion().getTimestamp() < currentTaskVersion.getVersion().getTimestamp()) {
            // It is an earlier version. Ignore it.
            titusRuntime.getCodeInvariants().inconsistent(
                    "Received earlier version of a task: current=%s, received=%s",
                    currentTaskVersion.getVersion().getTimestamp(), updatedTask.getVersion().getTimestamp()
            );
            return Optional.empty();
        }

        CachedServiceJob update = new CachedServiceJob(
                job,
                tasks.plus(taskId, updatedTask),
                titusRuntime
        );
        return Optional.ofNullable(snapshot.newSnapshot(
                snapshot.cachedJobsById.plus(job.getId(), update),
                snapshot.jobsById,
                snapshot.taskById.plus(taskId, updatedTask)
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

    @Override
    public Optional<JobSnapshot> removeTask(PCollectionJobSnapshot snapshot, Task task) {
        if (!tasks.containsKey(task.getId())) {
            return Optional.empty();
        }
        CachedServiceJob update = new CachedServiceJob(job, tasks.minus(task.getId()), titusRuntime);
        return Optional.ofNullable(snapshot.newSnapshot(
                snapshot.cachedJobsById.plus(job.getId(), update),
                snapshot.jobsById,
                snapshot.taskById.minus(task.getId())
        ));
    }

    public static CachedJob newServiceInstance(Job<?> job, PMap<String, Task> tasks, TitusRuntime titusRuntime) {
        // Filter out finished tasks.
        Map<String, Task> filtered = new HashMap<>();
        tasks.forEach((taskId, task) -> {
            if (task.getStatus().getState() != TaskState.Finished) {
                filtered.put(taskId, task);
            }
        });

        return new CachedServiceJob(job, HashTreePMap.from(filtered), titusRuntime);
    }
}
