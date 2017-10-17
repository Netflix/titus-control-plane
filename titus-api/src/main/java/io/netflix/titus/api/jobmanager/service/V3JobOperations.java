/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.api.jobmanager.service;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import io.netflix.titus.api.jobmanager.model.event.JobManagerEvent;
import io.netflix.titus.api.jobmanager.model.job.Capacity;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.common.util.tuple.Pair;
import rx.Completable;
import rx.Observable;

/**
 *
 */
public interface V3JobOperations {

    Observable<String> createJob(JobDescriptor<?> jobDescriptor);

    List<Job> getJobs();

    Optional<Job<?>> getJob(String jobId);

    List<Task> getTasks();

    List<Task> getTasks(String jobId);

    List<Job<?>> findJobs(Predicate<Pair<Job<?>, List<Task>>> queryPredicate, int offset, int limit);

    List<Pair<Job<?>, Task>> findTasks(Predicate<Pair<Job<?>, Task>> queryPredicate, int offset, int limit);

    default Optional<Pair<Job<?>, Task>> findTaskById(String taskId) {
        List<Pair<Job<?>, Task>> tasks = findTasks(jobTaskPair -> jobTaskPair.getRight().getId().equals(taskId), 0, 1);
        return tasks.isEmpty() ? Optional.empty() : Optional.of(tasks.get(0));
    }

    Observable<Void> updateJobCapacity(String jobId, Capacity capacity);

    Observable<Void> updateJobStatus(String serviceJobId, boolean enabled);

    Observable<Void> killJob(String jobId);

    Observable<Void> killTask(String taskId, boolean shrink, String reason);

    /**
     * Applies the provided update function to a task before persisting it to a store. In case of system failure
     * the update may be lost.
     */
    Completable updateTask(String taskId, Function<Task, Task> changeFunction, String reason);

    /**
     * Applies the provided update function to a task, and persists it in the store before updating the internal model.
     * This call guarantees system consistency in case of crashes/failovers.
     */
    Completable updateTaskAfterStore(String taskId, Function<Task, Task> changeFunction);

    Observable<JobManagerEvent> observeJobs();

    Observable<JobManagerEvent> observeJob(String jobId);
}
