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

package com.netflix.titus.supplementary.relocation.workflow.step;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Stopwatch;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan;
import com.netflix.titus.api.relocation.model.TaskRelocationStatus;
import com.netflix.titus.api.relocation.model.TaskRelocationStatus.TaskRelocationState;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.DateTimeExt;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.common.util.code.CodeInvariants;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.runtime.connector.eviction.EvictionServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

/**
 * In this step, all tasks that were selected for termination, are terminated.
 */
public class TaskEvictionStep {

    private static final Logger logger = LoggerFactory.getLogger(TaskEvictionStep.class);

    private static final int CONCURRENCY_LIMIT = 20;

    private static final Duration EVICTION_TIMEOUT = Duration.ofSeconds(5);

    private static final String STEP_NAME = "taskEvictionStep";

    private final EvictionServiceClient evictionServiceClient;
    private final CodeInvariants invariants;
    private final RelocationTransactionLogger transactionLog;
    private final Scheduler scheduler;
    private final Clock clock;
    private final StepMetrics metrics;

    public TaskEvictionStep(EvictionServiceClient evictionServiceClient,
                            TitusRuntime titusRuntime,
                            RelocationTransactionLogger transactionLog,
                            Scheduler scheduler) {
        this.evictionServiceClient = evictionServiceClient;
        this.transactionLog = transactionLog;
        this.scheduler = scheduler;
        this.invariants = titusRuntime.getCodeInvariants();
        this.clock = titusRuntime.getClock();
        this.metrics = new StepMetrics(STEP_NAME, titusRuntime);
    }

    public Map<String, TaskRelocationStatus> evict(Map<String, TaskRelocationPlan> taskToEvict) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            Map<String, TaskRelocationStatus> result = execute(taskToEvict);
            metrics.onSuccess(result.size(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
            logger.debug("Eviction result: {}", result);
            return result;
        } catch (Exception e) {
            logger.error("Step processing error", e);
            metrics.onError(stopwatch.elapsed(TimeUnit.MILLISECONDS));
            throw e;
        }
    }

    private Map<String, TaskRelocationStatus> execute(Map<String, TaskRelocationPlan> taskToEvict) {
        Map<String, Mono<Void>> actions = taskToEvict.values().stream()
                .collect(Collectors.toMap(
                        TaskRelocationPlan::getTaskId,
                        p -> {
                            String message;
                            switch (p.getReason()) {
                                case AgentEvacuation:
                                    message = String.format("Agent evacuation: %s", p.getReasonMessage());
                                    break;
                                case SelfManagedMigration:
                                    message = String.format("Self managed migration requested on %s: %s", DateTimeExt.toUtcDateTimeString(p.getDecisionTime()), p.getReasonMessage());
                                    break;
                                case TaskMigration:
                                    message = p.getReasonMessage();
                                    break;
                                default:
                                    message = String.format("[unrecognized relocation reason %s]: %s" + p.getReason(), p.getReasonMessage());
                            }
                            return evictionServiceClient.terminateTask(p.getTaskId(), message).timeout(EVICTION_TIMEOUT);
                        }));

        Map<String, Optional<Throwable>> evictionResults;
        try {
            evictionResults = ReactorExt.merge(actions, CONCURRENCY_LIMIT, scheduler).block();
        } catch (Exception e) {
            logger.warn("Unexpected error when calling the eviction service", e);
            return taskToEvict.values().stream()
                    .map(p -> TaskRelocationStatus.newBuilder()
                            .withState(TaskRelocationState.Failure)
                            .withStatusCode(TaskRelocationStatus.STATUS_SYSTEM_ERROR)
                            .withStatusMessage("Unexpected error: " + ExceptionExt.toMessageChain(e))
                            .withTimestamp(clock.wallTime())
                            .build()
                    )
                    .collect(Collectors.toMap(TaskRelocationStatus::getTaskId, s -> s));
        }
        Map<String, TaskRelocationStatus> results = new HashMap<>();

        taskToEvict.forEach((taskId, plan) -> {
            Optional<Throwable> evictionResult = evictionResults.get(plan.getTaskId());

            TaskRelocationStatus status;
            if (evictionResult != null) {
                if (!evictionResult.isPresent()) {
                    status = TaskRelocationStatus.newBuilder()
                            .withTaskId(taskId)
                            .withState(TaskRelocationState.Success)
                            .withStatusCode(TaskRelocationStatus.STATUS_CODE_TERMINATED)
                            .withStatusMessage("Task terminated successfully")
                            .withTaskRelocationPlan(plan)
                            .withTimestamp(clock.wallTime())
                            .build();
                } else {
                    status = TaskRelocationStatus.newBuilder()
                            .withTaskId(taskId)
                            .withState(TaskRelocationState.Failure)
                            .withStatusCode(TaskRelocationStatus.STATUS_EVICTION_ERROR)
                            .withStatusMessage(evictionResult.get().getMessage())
                            .withTaskRelocationPlan(plan)
                            .withTimestamp(clock.wallTime())
                            .build();
                }
            } else {
                // This should never happen
                invariants.inconsistent("Eviction result missing: taskId=%s", plan.getTaskId());
                status = TaskRelocationStatus.newBuilder()
                        .withTaskId(taskId)
                        .withState(TaskRelocationState.Failure)
                        .withStatusCode(TaskRelocationStatus.STATUS_SYSTEM_ERROR)
                        .withStatusMessage("Eviction result missing")
                        .withTaskRelocationPlan(plan)
                        .withTimestamp(clock.wallTime())
                        .build();
            }
            results.put(taskId, status);

            transactionLog.logTaskRelocationStatus(STEP_NAME, "eviction", status);
        });

        return results;
    }
}
