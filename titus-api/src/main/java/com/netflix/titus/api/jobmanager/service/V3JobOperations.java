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

package com.netflix.titus.api.jobmanager.service;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobProcesses;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.common.util.tuple.Pair;
import rx.Completable;
import rx.Observable;

import static com.netflix.titus.common.util.FunctionExt.alwaysTrue;

public interface V3JobOperations {

    String COMPONENT = "jobManagement";

    enum Trigger {
        API,
        Mesos,
        Reconciler,
        Scheduler,
        TaskMigration,
    }

    Observable<String> createJob(JobDescriptor<?> jobDescriptor);

    List<Job> getJobs();

    Optional<Job<?>> getJob(String jobId);

    List<Task> getTasks();

    List<Task> getTasks(String jobId);

    List<Pair<Job, List<Task>>> getJobsAndTasks();

    List<Job<?>> findJobs(Predicate<Pair<Job<?>, List<Task>>> queryPredicate, int offset, int limit);

    List<Pair<Job<?>, Task>> findTasks(Predicate<Pair<Job<?>, Task>> queryPredicate, int offset, int limit);

    Optional<Pair<Job<?>, Task>> findTaskById(String taskId);

    Observable<Void> updateJobCapacity(String jobId, Capacity capacity);

    Observable<Void> updateServiceJobProcesses(String jobId, ServiceJobProcesses serviceJobProcesses);

    Observable<Void> updateJobStatus(String serviceJobId, boolean enabled);

    Observable<Void> killJob(String jobId);

    Observable<Void> killTask(String taskId, boolean shrink, String reason);

    /**
     * Applies the provided update function to a task before persisting it to a store. In case of system failure
     * the update may be lost.
     */
    Completable updateTask(String taskId, Function<Task, Optional<Task>> changeFunction, Trigger trigger, String reason);

    /**
     * Called by scheduler when a task is assigned to an agent. The new task state is written to store first, and next
     * internal models are updated.
     * <p>
     * TODO 'Launched' state means two things today. Task placement by Fenzo, and Mesos 'Launched'. It makes sense to separate the two.
     */
    Completable recordTaskPlacement(String taskId, Function<Task, Task> changeFunction);

    default Observable<JobManagerEvent<?>> observeJobs() {
        return observeJobs(alwaysTrue(), alwaysTrue());
    }

    Observable<JobManagerEvent<?>> observeJobs(Predicate<Pair<Job<?>, List<Task>>> jobsPredicate,
                                               Predicate<Pair<Job<?>, Task>> tasksPredicate);

    Observable<JobManagerEvent<?>> observeJob(String jobId);
}
