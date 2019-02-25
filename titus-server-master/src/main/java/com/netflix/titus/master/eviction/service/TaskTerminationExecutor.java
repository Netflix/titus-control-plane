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

import com.netflix.titus.api.eviction.model.event.EvictionEvent;
import com.netflix.titus.api.eviction.service.EvictionException;
import com.netflix.titus.api.jobmanager.model.CallMetadata;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.rx.invoker.ReactorSerializedInvoker;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.eviction.service.quota.ConsumptionResult;
import com.netflix.titus.master.eviction.service.quota.TitusQuotasManager;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

class TaskTerminationExecutor {

    private static final Duration TASK_TERMINATE_TIMEOUT = Duration.ofSeconds(5);

    private static final Duration TASK_EXCESSIVE_RUNNING_TIMEOUT = Duration.ofSeconds(60);

    private static final int MAX_QUEUE_SIZE = 50;

    private final V3JobOperations jobOperations;
    private final TitusQuotasManager quotasManager;
    private final ReactorSerializedInvoker<Void> serializedInvoker;

    private final DirectProcessor<EvictionEvent> eventProcessor = DirectProcessor.create();
    private final EvictionTransactionLog transactionLog;
    private final TaskTerminationExecutorMetrics metrics;

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
        this.metrics = new TaskTerminationExecutorMetrics(titusRuntime);
        this.transactionLog = new EvictionTransactionLog();
    }

    void shutdown() {
        serializedInvoker.shutdown(Duration.ofSeconds(30));
    }

    Flux<EvictionEvent> events() {
        return eventProcessor;
    }

    public Mono<Void> terminateTask(String taskId, String reason, String callerId) {
        return findAndVerifyJobAndTask(taskId, reason, callerId)
                .flatMap(jobTaskPair -> {
                    Job<?> job = jobTaskPair.getLeft();
                    Task task = jobTaskPair.getRight();

                    return serializedInvoker
                            .submit(doTerminateTask(taskId, reason, job, task, callerId))
                            .doOnSuccess(next -> onSuccessfulTermination(job, taskId, reason, callerId))
                            .doOnError(error -> onTerminationError(job, taskId, reason, callerId, error));
                });
    }

    private Mono<Pair<Job<?>, Task>> findAndVerifyJobAndTask(String taskId, String reason, String callerId) {
        return Mono
                .defer(() -> Mono.just(checkTaskIsRunningOrThrowAnException(taskId)))
                .doOnError(error -> onValidationError(taskId, reason, callerId, error));
    }

    private Mono<Void> doTerminateTask(String taskId, String reason, Job<?> job, Task task, String callerId) {
        return Mono.defer(() -> {
            ConsumptionResult consumptionResult = quotasManager.tryConsumeQuota(job, task);

            return consumptionResult.isApproved()
                    ? ReactorExt.toMono(jobOperations.killTask(taskId, false, reason, CallMetadata.newBuilder().withCallerId(callerId).withCallReason(reason).build())).timeout(TASK_TERMINATE_TIMEOUT)
                    : Mono.error(EvictionException.noAvailableJobQuota(job, consumptionResult.getRejectionReason().get()));
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

    /**
     * When validation error happens, we do not emit any event.
     */
    private void onValidationError(String taskId, String reason, String callerId, Throwable error) {
        metrics.error(error);
        transactionLog.logTaskTerminationUnexpectedError(taskId, reason, callerId, error);
    }

    private void onSuccessfulTermination(Job<?> job, String taskId, String reason, String callerContext) {
        metrics.terminated();
        transactionLog.logTaskTermination(job, taskId, reason, callerContext);
        eventProcessor.onNext(EvictionEvent.newSuccessfulTaskTerminationEvent(taskId, reason));
    }

    private void onTerminationError(Job<?> job, String taskId, String reason, String callerId, Throwable error) {
        metrics.error(error);
        transactionLog.logTaskTerminationError(job, taskId, reason, callerId, error);
        eventProcessor.onNext(EvictionEvent.newFailedTaskTerminationEvent(taskId, reason, error));
    }
}
