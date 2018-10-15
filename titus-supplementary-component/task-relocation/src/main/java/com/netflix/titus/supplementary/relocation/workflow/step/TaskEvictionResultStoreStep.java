package com.netflix.titus.supplementary.relocation.workflow.step;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;

import com.netflix.titus.supplementary.relocation.model.TaskRelocationPlan;
import com.netflix.titus.supplementary.relocation.model.TaskRelocationStatus;
import com.netflix.titus.supplementary.relocation.store.TaskRelocationArchiveStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * At this step, the task eviction result is written to the database.
 */
public class TaskEvictionResultStoreStep {

    private static final Logger logger = LoggerFactory.getLogger(TaskEvictionResultStoreStep.class);

    private static final Duration STORE_UPDATE_TIMEOUT = Duration.ofSeconds(30);

    private final TaskRelocationArchiveStore store;

    public TaskEvictionResultStoreStep(TaskRelocationArchiveStore store) {
        this.store = store;
    }

    public void storeTaskEvictionResults(Map<String, TaskRelocationPlan> taskEvictionPlans,
                                         Map<String, TaskRelocationStatus> taskEvictionResults) {
        Map<String, Optional<Throwable>> result;
        try {
            result = store.createTaskRelocationStatuses(new ArrayList<>(taskEvictionResults.values()))
                    .timeout(STORE_UPDATE_TIMEOUT)
                    .block();
        } catch (Exception e) {
            logger.warn("Could not remove task relocation plans from the database: {}", taskEvictionResults.keySet(), e);
            return;
        }

        result.forEach((taskId, errorOpt) -> {
            if (errorOpt.isPresent()) {
                logger.warn("Failed to store the task relocation result in the archive store: taskId={}, error={}", taskId, errorOpt.get().getMessage());
            } else {
                logger.info("Stored the task relocation plan in the archive store: taskId={}", taskId);
            }
        });
    }
}
