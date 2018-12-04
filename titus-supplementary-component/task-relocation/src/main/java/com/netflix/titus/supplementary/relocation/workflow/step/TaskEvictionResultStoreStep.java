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
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.api.relocation.model.TaskRelocationStatus;
import com.netflix.titus.supplementary.relocation.store.TaskRelocationResultStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * At this step, the task eviction result is written to the database.
 */
public class TaskEvictionResultStoreStep {

    private static final Logger logger = LoggerFactory.getLogger(TaskEvictionResultStoreStep.class);

    private static final Duration STORE_UPDATE_TIMEOUT = Duration.ofSeconds(30);

    private static final String STEP_NAME = "taskEvictionResultStoreStep";

    private final TaskRelocationResultStore store;
    private final RelocationTransactionLogger transactionLog;
    private final StepMetrics metrics;

    public TaskEvictionResultStoreStep(TaskRelocationResultStore store, RelocationTransactionLogger transactionLog, TitusRuntime titusRuntime) {
        this.store = store;
        this.transactionLog = transactionLog;
        this.metrics = new StepMetrics(STEP_NAME, titusRuntime);
    }

    public void storeTaskEvictionResults(Map<String, TaskRelocationStatus> taskEvictionResults) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            int updates = execute(taskEvictionResults);
            metrics.onSuccess(updates, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        } catch (Exception e) {
            logger.error("Step processing error", e);
            metrics.onError(stopwatch.elapsed(TimeUnit.MILLISECONDS));
            throw e;
        }
    }

    private int execute(Map<String, TaskRelocationStatus> taskEvictionResults) {
        Map<String, Optional<Throwable>> result;
        try {
            result = store.createTaskRelocationStatuses(new ArrayList<>(taskEvictionResults.values()))
                    .timeout(STORE_UPDATE_TIMEOUT)
                    .block();
        } catch (Exception e) {
            logger.warn("Could not remove task relocation plans from the database: {}", taskEvictionResults.keySet(), e);
            taskEvictionResults.forEach((taskId, status) -> transactionLog.logTaskRelocationStatusStoreFailure(STEP_NAME, status, e));
            return 0;
        }

        result.forEach((taskId, errorOpt) -> {
            if (errorOpt.isPresent()) {
                transactionLog.logTaskRelocationStatusStoreFailure(STEP_NAME, taskEvictionResults.get(taskId), errorOpt.get());
            } else {
                transactionLog.logTaskRelocationStatus(STEP_NAME, "storeUpdate", taskEvictionResults.get(taskId));
            }
        });

        return result.size();
    }
}
