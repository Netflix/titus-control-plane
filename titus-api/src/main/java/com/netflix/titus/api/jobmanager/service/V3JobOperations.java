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
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobProcesses;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudget;
import com.netflix.titus.common.util.rx.ReactorExt;
import reactor.core.publisher.Mono;
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

    /**
     * @deprecated Use {@link #updateJobCapacityReactor(String, Capacity)}
     */
    Observable<Void> updateJobCapacity(String jobId, Capacity capacity);

    default Mono<Void> updateJobCapacityReactor(String jobId, Capacity capacity) {
        return ReactorExt.toMono(updateJobCapacity(jobId, capacity));
    }

    /**
     * @deprecated Use {@link #updateServiceJobProcessesReactor(String, ServiceJobProcesses)}
     */
    Observable<Void> updateServiceJobProcesses(String jobId, ServiceJobProcesses serviceJobProcesses);

    default Mono<Void> updateServiceJobProcessesReactor(String jobId, ServiceJobProcesses serviceJobProcesses) {
        return ReactorExt.toMono(updateServiceJobProcesses(jobId, serviceJobProcesses));
    }

    /**
     * @deprecated Use {@link #updateJobStatusReactor(String, boolean)}
     */
    Observable<Void> updateJobStatus(String serviceJobId, boolean enabled);

    default Mono<Void> updateJobStatusReactor(String serviceJobId, boolean enabled) {
        return ReactorExt.toMono(updateJobStatus(serviceJobId, enabled));
    }

    Mono<Void> updateJobDisruptionBudget(String jobId, DisruptionBudget disruptionBudget);

    Mono<Void> updateJobAttributes(String jobId, Map<String, String> attributes);

    Mono<Void> deleteJobAttributes(String jobId, List<String> keys);

    /**
     * @deprecated Use {@link #killJobReactor(String, String)}
     */
    Observable<Void> killJob(String jobId, String reason);

    default Mono<Void> killJobReactor(String jobId, String reason) {
        return ReactorExt.toMono(killJob(jobId, reason));
    }

    /**
     * @deprecated Use {@link #killTaskReactor(String, boolean, String)}
     */
    Observable<Void> killTask(String taskId, boolean shrink, String reason);

    default Mono<Void> killTaskReactor(String taskId, boolean shrink, String reason) {
        return ReactorExt.toMono(killTask(taskId, shrink, reason));
    }

    /**
     * Move a task from one service job to another.
     */
    Observable<Void> moveServiceTask(String sourceJobId, String targetJobId, String taskId);

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
