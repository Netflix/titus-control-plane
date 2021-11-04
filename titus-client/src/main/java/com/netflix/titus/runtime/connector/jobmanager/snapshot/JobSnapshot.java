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

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.runtime.connector.common.replicator.ReplicatedSnapshot;

/**
 * TODO Handle moved tasks
 * TODO Finished tasks are not handled correctly for batch jobs (they are in active data set until replaced).
 */
public abstract class JobSnapshot extends ReplicatedSnapshot {

    protected final String snapshotId;

    protected JobSnapshot(String snapshotId) {
        this.snapshotId = snapshotId;
    }

    public String getSnapshotId() {
        return snapshotId;
    }

    public abstract Map<String, Job<?>> getJobMap();

    /**
     * This value is expensive to compute on each update. {@link PCollectionJobSnapshot} computes it lazily to avoid
     * the overhead. Consider using {@link #getJobMap()}.
     */
    @Deprecated
    public abstract List<Job<?>> getJobs();

    public abstract Optional<Job<?>> findJob(String jobId);

    public abstract Map<String, Task> getTaskMap();

    /**
     * This value is expensive to compute on each update. {@link PCollectionJobSnapshot} computes it lazily to avoid
     * the overhead. Consider using {@link #getTaskMap()}.
     */
    @Deprecated
    public abstract List<Task> getTasks();

    public abstract Map<String, Task> getTasks(String jobId);

    /**
     * This value is expensive to compute on each update. {@link PCollectionJobSnapshot} computes it lazily to avoid
     * the overhead. Consider using other methods.
     */
    @Deprecated
    public abstract List<Pair<Job<?>, Map<String, Task>>> getJobsAndTasks();

    public abstract Optional<Pair<Job<?>, Task>> findTaskById(String taskId);

    public abstract Optional<JobSnapshot> updateJob(Job<?> job);

    public abstract Optional<JobSnapshot> removeArchivedJob(Job<?> job);

    public abstract Optional<JobSnapshot> updateTask(Task task, boolean moved);

    public abstract Optional<JobSnapshot> removeArchivedTask(Task task);
}
