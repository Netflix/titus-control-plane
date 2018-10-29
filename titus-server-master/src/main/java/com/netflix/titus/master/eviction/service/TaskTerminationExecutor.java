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

package com.netflix.titus.master.eviction.service;

import java.time.Duration;
import java.util.Optional;

import com.netflix.titus.api.eviction.model.EvictionQuota;
import com.netflix.titus.api.eviction.model.event.EvictionEvent;
import com.netflix.titus.api.eviction.service.EvictionException;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.rx.queue.ReactorSerializedInvoker;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.eviction.service.quota.TitusQuotasManager;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;

class TaskTerminationExecutor {

    private static final Duration TASK_TERMINATE_TIMEOUT = Duration.ofSeconds(5);

    private static final Duration TASK_EXCESSIVE_RUNNING_TIMEOUT = Duration.ofSeconds(60);

    private static final int MAX_QUEUE_SIZE = 50;

    private final V3JobOperations jobOperations;
    private final TitusQuotasManager quotasManager;
    private final ReactorSerializedInvoker<Void> serializedInvoker;

    private final EmitterProcessor<EvictionEvent> eventProcessor = EmitterProcessor.create();

    TaskTerminationExecutor(V3JobOperations jobOperations,
                            TitusQuotasManager quotasManager,
                            TitusRuntime titusRuntime,
                            Scheduler scheduler) {
        this.jobOperations = jobOperations;
        this.quotasManager = quotasManager;
        this.serializedInvoker = ReactorSerializedInvoker.<Void>newBuilder()
                .withName("taskTerminationExecutor")
                .withMaxQueueSize(MAX_QUEUE_SIZE)
                .withExcessiveRunningTime(TASK_EXCESSIVE_RUNNING_TIMEOUT)
                .withScheduler(scheduler)
                .withClock(titusRuntime.getClock())
                .withRegistry(titusRuntime.getRegistry())
                .build();
    }

    void shutdown() {
        serializedInvoker.shutdown(Duration.ofSeconds(30));
    }

    Flux<EvictionEvent> events() {
        return eventProcessor;
    }

    public Mono<Void> terminateTask(String taskId, String reason) {
        return Mono
                .defer(() -> {
                    Pair<Job<?>, Task> jobTaskPair = checkTaskIsRunningOrThrowAnException(taskId);
                    Job<?> job = jobTaskPair.getLeft();
                    Task task = jobTaskPair.getRight();

                    if (getQuota(job) <= 0) {
                        return Mono.error(newEvictionRejectionEvent(taskId, reason, job));
                    }

                    return serializedInvoker.submit(Mono.defer(() -> {
                        if (!quotasManager.tryConsumeQuota(job, task)) {
                            return Mono.error(newEvictionRejectionEvent(taskId, reason, job));
                        }
                        return ReactorExt.toMono(jobOperations.killTask(taskId, false, reason)).timeout(TASK_TERMINATE_TIMEOUT);
                    }));
                })
                .doFinally(signalType -> {
                    if (signalType == SignalType.ON_COMPLETE) {
                        eventProcessor.onNext(EvictionEvent.newTaskTerminationEvent(taskId, reason, true));
                    }
                });
    }

    private Pair<Job<?>, Task> checkTaskIsRunningOrThrowAnException(String taskId) {
        Optional<Pair<Job<?>, Task>> jobAndTask = jobOperations.findTaskById(taskId);
        if (!jobAndTask.isPresent()) {
            throw EvictionException.taskNotFound(taskId);
        }
        Task task = jobAndTask.get().getRight();
        TaskState state = task.getStatus().getState();
        if (state == TaskState.Accepted) {
            throw EvictionException.taskNotScheduledYet(task);
        }
        if (!TaskState.isBefore(state, TaskState.KillInitiated)) {
            throw EvictionException.taskAlreadyStopped(task);
        }
        return jobAndTask.get();
    }

    private Long getQuota(Job<?> job) {
        return quotasManager.findJobEvictionQuota(job.getId()).map(EvictionQuota::getQuota).orElse(0L);
    }

    private EvictionException newEvictionRejectionEvent(String taskId, String reason, Job<?> job) {
        String constraintReason = quotasManager.explainJobQuotaConstraints(job.getId()).orElse("Job no longer around");
        eventProcessor.onNext(EvictionEvent.newTaskTerminationEvent(
                taskId,
                String.format("Eviction request rejected: constraint=%s, evictionReason=%s", constraintReason, reason),
                false
        ));
        return EvictionException.noAvailableJobQuota(job, constraintReason);
    }
}
