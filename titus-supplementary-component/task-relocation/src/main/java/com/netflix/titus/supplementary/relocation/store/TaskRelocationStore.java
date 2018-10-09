package com.netflix.titus.supplementary.relocation.store;

import java.util.Map;

import com.netflix.titus.supplementary.relocation.model.TaskRelocationPlan;
import com.netflix.titus.supplementary.relocation.model.TaskRelocationStatus;
import reactor.core.publisher.Mono;

/**
 * Store API for managing the active data set. This includes active task relocation plans, and the latest
 * relocation attempts.
 */
public interface TaskRelocationStore {

    /**
     * Creates or updates a record in the database.
     */
    Mono<Void> createOrUpdateTaskRelocationPlan(TaskRelocationPlan taskRelocationPlan);

    /**
     * Creates or updates a record in the database.
     */
    Mono<Void> createTaskRelocationStatus(TaskRelocationStatus taskRelocationStatus);

    /**
     * Returns all task relocation plans stored in the database.
     */
    Mono<Map<String, TaskRelocationPlan>> getAllTaskRelocationPlans();

    /**
     * Returns all task relocation statuses. Each task relocation status is associated with exactly one
     * task relocation plan. Only the latest relocation status is stored in this store. Previous states (failed) are
     * stored in the archive store (see {@link TaskRelocationArchiveStore}).
     */
    Mono<Map<String, TaskRelocationStatus>> getAllTaskRelocationStatuses();
}
