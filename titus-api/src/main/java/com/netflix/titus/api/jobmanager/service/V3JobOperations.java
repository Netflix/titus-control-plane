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

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import com.netflix.titus.api.jobmanager.model.CallMetadata;
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

    Observable<String> createJob(JobDescriptor<?> jobDescriptor, CallMetadata callMetadata);

    /**
     * @deprecated Use {@link #updateJobCapacityReactor(String, Capacity, CallMetadata)}
     */
    Observable<Void> updateJobCapacity(String jobId, Capacity capacity, CallMetadata callMetadata);

    default Mono<Void> updateJobCapacityReactor(String jobId, Capacity capacity, CallMetadata callMetadata) {
        return ReactorExt.toMono(updateJobCapacity(jobId, capacity, callMetadata));
    }

    /**
     * @deprecated Use {@link #updateServiceJobProcessesReactor(String, ServiceJobProcesses, CallMetadata)}
     */
    Observable<Void> updateServiceJobProcesses(String jobId, ServiceJobProcesses serviceJobProcesses, CallMetadata callMetadata);

    default Mono<Void> updateServiceJobProcessesReactor(String jobId, ServiceJobProcesses serviceJobProcesses, CallMetadata callMetadata) {
        return ReactorExt.toMono(updateServiceJobProcesses(jobId, serviceJobProcesses, callMetadata));
    }

    /**
     * @deprecated Use {@link #updateJobStatusReactor(String, boolean, CallMetadata)}
     */
    Observable<Void> updateJobStatus(String serviceJobId, boolean enabled, CallMetadata callMetadata);

    default Mono<Void> updateJobStatusReactor(String serviceJobId, boolean enabled, CallMetadata callMetadata) {
        return ReactorExt.toMono(updateJobStatus(serviceJobId, enabled, callMetadata));
    }

    Mono<Void> updateJobDisruptionBudget(String jobId, DisruptionBudget disruptionBudget, CallMetadata callMetadata);

    Mono<Void> updateJobAttributes(String jobId, Map<String, String> attributes, CallMetadata callMetadata);

    Mono<Void> deleteJobAttributes(String jobId, Set<String> keys, CallMetadata callMetadata);

    /**
     * @deprecated Use {@link #killJobReactor(String, String, CallMetadata)}
     */
    Observable<Void> killJob(String jobId, String reason, CallMetadata callMetadata);

    // TODO: get rid of the reason
    default Mono<Void> killJobReactor(String jobId, String reason, CallMetadata callMetadata) {
        return ReactorExt.toMono(killJob(jobId, reason, callMetadata));
    }

    /**
     * @deprecated Use {@link #killTaskReactor(String, boolean, String, CallMetadata)}
     */
    Observable<Void> killTask(String taskId, boolean shrink, String reason, CallMetadata callMetadata);

    // TODO: get rid of the reason
    default Mono<Void> killTaskReactor(String taskId, boolean shrink, String reason, CallMetadata callMetadata) {
        return ReactorExt.toMono(killTask(taskId, shrink, reason, callMetadata));
    }

    /**
     * Move a task from one service job to another.
     */
    Observable<Void> moveServiceTask(String sourceJobId, String targetJobId, String taskId, CallMetadata callMetadata);

    /**
     * Applies the provided update function to a task before persisting it to a store. In case of system failure
     * the update may be lost.
     */
    Completable updateTask(String taskId, Function<Task, Optional<Task>> changeFunction, Trigger trigger, String reason, CallMetadata callMetadata);

    /**
     * Called by scheduler when a task is assigned to an agent. The new task state is written to store first, and next
     * internal models are updated.
     * <p>
     * TODO 'Launched' state means two things today. Task placement by Fenzo, and Mesos 'Launched'. It makes sense to separate the two.
     */
    Completable recordTaskPlacement(String taskId, Function<Task, Task> changeFunction, CallMetadata callMetadata);
}
