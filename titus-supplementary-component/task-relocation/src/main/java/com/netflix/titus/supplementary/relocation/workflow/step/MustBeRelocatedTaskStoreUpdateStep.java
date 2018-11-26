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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Stopwatch;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.api.relocation.model.RelocationFunctions;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan;
import com.netflix.titus.supplementary.relocation.store.TaskRelocationStore;
import com.netflix.titus.supplementary.relocation.workflow.RelocationWorkflowException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Step at which information about task that must be relocated is persisted in the database.
 */
public class MustBeRelocatedTaskStoreUpdateStep {

    private static final Logger logger = LoggerFactory.getLogger(MustBeRelocatedTaskStoreUpdateStep.class);

    private static final Duration STORE_UPDATE_TIMEOUT = Duration.ofSeconds(30);

    private static final String STEP_NAME = "mustBeRelocatedTaskStoreUpdateStep";

    private final TaskRelocationStore store;
    private final RelocationTransactionLogger transactionLog;
    private final StepMetrics metrics;

    private Map<String, TaskRelocationPlan> relocationsPlanInStore;

    public MustBeRelocatedTaskStoreUpdateStep(TaskRelocationStore store, RelocationTransactionLogger transactionLog, TitusRuntime titusRuntime) {
        this.store = store;
        this.transactionLog = transactionLog;
        this.relocationsPlanInStore = new HashMap<>(loadPlanFromStore());
        this.metrics = new StepMetrics(STEP_NAME, titusRuntime);
    }

    public void persistChangesInStore(Map<String, TaskRelocationPlan> mustBeRelocatedTasks) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            int updates = execute(mustBeRelocatedTasks);
            metrics.onSuccess(updates, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        } catch (Exception e) {
            logger.error("Step processing error", e);
            metrics.onError(stopwatch.elapsed(TimeUnit.MILLISECONDS));
            throw e;
        }
    }

    private int execute(Map<String, TaskRelocationPlan> mustBeRelocatedTasks) {
        Set<String> toRemove = CollectionsExt.copyAndRemove(relocationsPlanInStore.keySet(), mustBeRelocatedTasks.keySet());

        List<TaskRelocationPlan> toUpdate = mustBeRelocatedTasks.values().stream()
                .filter(current -> areDifferent(relocationsPlanInStore.get(current.getTaskId()), current))
                .collect(Collectors.toList());

        removeFromStore(toRemove);
        updateInStore(toUpdate);

        logger.debug("Plans removed from store: {}", toRemove);
        logger.debug("Plans added to store: {}", toUpdate.stream().map(TaskRelocationPlan::getTaskId).collect(Collectors.toList()));

        relocationsPlanInStore.keySet().removeAll(toRemove);
        toUpdate.forEach(plan -> relocationsPlanInStore.put(plan.getTaskId(), plan));

        return toRemove.size() + toUpdate.size();
    }

    private Map<String, TaskRelocationPlan> loadPlanFromStore() {
        try {
            Map<String, TaskRelocationPlan> allPlans = store.getAllTaskRelocationPlans().block();
            allPlans.forEach((taskId, plan) -> transactionLog.logRelocationReadFromStore(STEP_NAME, plan));
            return allPlans;
        } catch (Exception e) {
            throw RelocationWorkflowException.storeError("Cannot load task relocation plan from store on startup", e);
        }
    }

    private boolean areDifferent(TaskRelocationPlan previous, TaskRelocationPlan current) {
        return previous == null || !RelocationFunctions.areEqualExceptRelocationTime(previous, current);
    }

    private void updateInStore(List<TaskRelocationPlan> toUpdate) {
        if (toUpdate.isEmpty()) {
            return;
        }

        Map<String, TaskRelocationPlan> byTaskId = toUpdate.stream().collect(Collectors.toMap(TaskRelocationPlan::getTaskId, p -> p));

        Map<String, Optional<Throwable>> result;
        try {
            result = store.createOrUpdateTaskRelocationPlans(toUpdate)
                    .timeout(STORE_UPDATE_TIMEOUT)
                    .block();
        } catch (Exception e) {
            List<String> toUpdateIds = toUpdate.stream().map(TaskRelocationPlan::getTaskId).collect(Collectors.toList());
            logger.warn("Could not remove task relocation plans from the database: {}", toUpdateIds, e);
            return;
        }

        result.forEach((taskId, errorOpt) -> {
            if (errorOpt.isPresent()) {
                transactionLog.logRelocationPlanUpdateInStoreError(STEP_NAME, byTaskId.get(taskId), errorOpt.get());
            } else {
                transactionLog.logRelocationPlanUpdatedInStore(STEP_NAME, byTaskId.get(taskId));
            }
        });
    }

    private void removeFromStore(Set<String> toRemove) {
        if (toRemove.isEmpty()) {
            return;
        }

        Map<String, Optional<Throwable>> result;
        try {
            result = store.removeTaskRelocationPlans(toRemove)
                    .timeout(STORE_UPDATE_TIMEOUT)
                    .block();
        } catch (Exception e) {
            logger.warn("Could not remove task relocation plans from the database: {}", toRemove, e);
            return;
        }

        result.forEach((taskId, errorOpt) -> {
            if (errorOpt.isPresent()) {
                transactionLog.logRelocationPlanRemoveFromStoreError(STEP_NAME, taskId, errorOpt.get());
            } else {
                transactionLog.logRelocationPlanRemovedFromStore(STEP_NAME, taskId);
            }
        });
    }
}
