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
import java.util.stream.Collectors;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.DateTimeExt;
import com.netflix.titus.common.util.code.CodeInvariants;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.runtime.connector.eviction.EvictionServiceClient;
import com.netflix.titus.supplementary.relocation.model.TaskRelocationPlan;
import com.netflix.titus.supplementary.relocation.model.TaskRelocationStatus;
import com.netflix.titus.supplementary.relocation.model.TaskRelocationStatus.TaskRelocationState;
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

    private final EvictionServiceClient evictionServiceClient;
    private final CodeInvariants invariants;
    private final Scheduler scheduler;

    public TaskEvictionStep(EvictionServiceClient evictionServiceClient, TitusRuntime titusRuntime, Scheduler scheduler) {
        this.evictionServiceClient = evictionServiceClient;
        this.scheduler = scheduler;
        this.invariants = titusRuntime.getCodeInvariants();
    }

    public Map<String, TaskRelocationStatus> evict(Map<String, TaskRelocationPlan> taskToEvict) {
        Map<String, Mono<Void>> actions = taskToEvict.values().stream()
                .collect(Collectors.toMap(
                        TaskRelocationPlan::getTaskId,
                        p -> {
                            String message = String.format("%s: reasonCode=%s, plannedRelocationTime=%s",
                                    p.getReasonMessage(), p.getReason(), DateTimeExt.toUtcDateTimeString(p.getRelocationTime())
                            );
                            return ReactorExt.toMono(evictionServiceClient.terminateTask(p.getTaskId(), message)).timeout(EVICTION_TIMEOUT);
                        }));

        Map<String, Optional<Throwable>> evictionResults;
        try {
            evictionResults = ReactorExt.merge(actions, CONCURRENCY_LIMIT, scheduler).block();
        } catch (Exception e) {
            logger.warn("Unexpected error when calling the eviction service", e);
            return taskToEvict.values().stream()
                    .map(p -> TaskRelocationStatus.newBuilder()
                            .withState(TaskRelocationState.Failure)
                            .withReasonCode("storeError")
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
                            .withState(TaskRelocationState.Success)
                            .withReasonCode("terminated")
                            .build();
                } else {
                    status = TaskRelocationStatus.newBuilder()
                            .withState(TaskRelocationState.Failure)
                            .withReasonCode(evictionResult.get().getMessage())
                            .build();
                }
            } else {
                // This should never happen
                invariants.inconsistent("Eviction result missing: taskId=%s", plan.getTaskId());
                status = TaskRelocationStatus.newBuilder()
                        .withState(TaskRelocationState.Failure)
                        .withReasonCode("unknownEvictionStatus")
                        .build();
            }
            results.put(taskId, status);

        });

        return results;
    }
}
