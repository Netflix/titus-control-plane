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

import java.util.Map;
import java.util.Optional;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.tuple.Pair;
import org.pcollections.PMap;

abstract class CachedJob {

    protected final Job<?> job;
    protected final PMap<String, Task> tasks;
    protected final Pair<Job<?>, Map<String, Task>> jobTasksPair;
    protected final TitusRuntime titusRuntime;

    protected CachedJob(Job<?> job, PMap<String, Task> tasks, TitusRuntime titusRuntime) {
        this.job = job;
        this.tasks = tasks;
        this.jobTasksPair = Pair.of(job, tasks);
        this.titusRuntime = titusRuntime;
    }

    public Job<?> getJob() {
        return job;
    }

    public PMap<String, Task> getTasks() {
        return tasks;
    }

    public Pair<Job<?>, Map<String, Task>> getJobTasksPair() {
        return jobTasksPair;
    }

    public abstract Optional<JobSnapshot> updateJob(PCollectionJobSnapshot snapshot, Job<?> job);

    public abstract Optional<JobSnapshot> updateTask(PCollectionJobSnapshot snapshot, Task task);

    public abstract Optional<JobSnapshot> removeJob(PCollectionJobSnapshot snapshot, Job<?> job);

    public abstract Optional<JobSnapshot> removeTask(PCollectionJobSnapshot snapshot, Task task);

    public static CachedJob newInstance(Job<?> job, PMap<String, Task> tasks, boolean archiveMode, TitusRuntime titusRuntime) {
        if (JobFunctions.isServiceJob(job)) {
            return CachedServiceJob.newServiceInstance(job, tasks, archiveMode, titusRuntime);
        }
        int size = JobFunctions.getJobDesiredSize(job);
        if (size == 1) {
            return CachedBatchJobWithOneTask.newBatchInstance((Job<BatchJobExt>) job, tasks, titusRuntime);
        }
        return CachedBatchJob.newBatchInstance((Job<BatchJobExt>) job, tasks, titusRuntime);
    }
}
