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
