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

import java.util.Optional;
import java.util.function.Function;

import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobProcesses;
import com.netflix.titus.api.jobmanager.model.job.Task;
import rx.Completable;
import rx.Observable;

public interface V3JobOperations extends ReadOnlyJobOperations {

    String COMPONENT = "jobManagement";

    enum Trigger {
        API,
        Mesos,
        Reconciler,
        Scheduler,
        TaskMigration,
    }

    Observable<String> createJob(JobDescriptor<?> jobDescriptor);

    Observable<Void> updateJobCapacity(String jobId, Capacity capacity);

    Observable<Void> updateServiceJobProcesses(String jobId, ServiceJobProcesses serviceJobProcesses);

    Observable<Void> updateJobStatus(String serviceJobId, boolean enabled);

    Observable<Void> killJob(String jobId);

    Observable<Void> killTask(String taskId, boolean shrink, String reason);

    /**
     * Mave a task from one service job to another.
     */
    Observable<Void> moveServiceTask(String taskId, String targetJobId);

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
}
