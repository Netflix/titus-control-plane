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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.supplementary.relocation.model.TaskRelocationPlan;
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

    private final TaskRelocationStore store;

    private Map<String, TaskRelocationPlan> relocationsPlanInStore;

    public MustBeRelocatedTaskStoreUpdateStep(TaskRelocationStore store) {
        this.store = store;
        this.relocationsPlanInStore = loadPlanFromStore();
    }

    public void persistChangesInStore(Map<String, TaskRelocationPlan> mustBeRelocatedTasks) {
        Set<String> toRemove = CollectionsExt.copyAndRemove(relocationsPlanInStore.keySet(), mustBeRelocatedTasks.keySet());

        List<TaskRelocationPlan> toUpdate = mustBeRelocatedTasks.values().stream()
                .filter(current -> areDifferent(relocationsPlanInStore.get(current.getTaskId()), current))
                .collect(Collectors.toList());

        removeFromStore(toRemove);
        updateInStore(toUpdate);
    }

    private Map<String, TaskRelocationPlan> loadPlanFromStore() {
        try {
            return store.getAllTaskRelocationPlans().block();
        } catch (Exception e) {
            throw RelocationWorkflowException.storeError("Cannot load task relocation plan from store on startup", e);
        }
    }

    private boolean areDifferent(TaskRelocationPlan previous, TaskRelocationPlan current) {
        return previous == null || !previous.equals(current);
    }

    private void updateInStore(List<TaskRelocationPlan> toUpdate) {
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
                logger.warn("Failed to store task relocation plan in store: taskId={}, error={}", taskId, errorOpt.get().getMessage());
            } else {
                logger.info("Stored task relocation plan in store: taskId={}", taskId);
            }
        });
    }

    private void removeFromStore(Set<String> toRemove) {
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
                logger.warn("Failed to remove outdated task relocation plan from store: taskId={}, error={}", taskId, errorOpt.get().getMessage());
            } else {
                logger.info("Removed outdated task relocation plan from store: taskId={}", taskId);
            }
        });
    }
}
