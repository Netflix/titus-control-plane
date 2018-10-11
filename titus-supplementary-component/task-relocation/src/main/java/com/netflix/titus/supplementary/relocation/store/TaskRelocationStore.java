package com.netflix.titus.supplementary.relocation.store;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.netflix.titus.supplementary.relocation.model.TaskRelocationPlan;
import reactor.core.publisher.Mono;

/**
 * Store API for managing the active data set. This includes active task relocation plans, and the latest
 * relocation attempts.
 */
public interface TaskRelocationStore {

    /**
     * Creates or updates task relocation plans in the database.
     */
    Mono<Map<String, Optional<Throwable>>> createOrUpdateTaskRelocationPlans(List<TaskRelocationPlan> taskRelocationPlans);

    /**
     * Returns all task relocation plans stored in the database.
     */
    Mono<Map<String, TaskRelocationPlan>> getAllTaskRelocationPlans();

    /**
     * Remove task relocation plans from the store.
     */
    Mono<Map<String, Optional<Throwable>>> removeTaskRelocationPlans(Set<String> toRemove);
}
