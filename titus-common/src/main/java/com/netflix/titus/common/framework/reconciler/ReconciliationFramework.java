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

package com.netflix.titus.common.framework.reconciler;

import java.util.List;
import java.util.Optional;

import com.netflix.titus.common.util.tuple.Pair;
import rx.Completable;
import rx.Observable;

/**
 * {@link ReconciliationFramework} manages lifecycle of multiple {@link ReconciliationEngine} instances, as well
 * as orchestrates their reconciliation processes.
 */
public interface ReconciliationFramework<EVENT> {

    /**
     * Starts the framework
     */
    void start();

    /**
     * Stops the reconciliation framework
     *
     * @return true if it was stopped in the specified timeout
     */
    boolean stop(long timeoutMs);

    /**
     * Event stream of changes in the engine.
     */
    Observable<EVENT> events();

    /**
     * @return {@link ReconciliationEngine} with root node having the given id or {@link Optional#empty()}.
     */
    Optional<ReconciliationEngine<EVENT>> findEngineByRootId(String id);

    /**
     * @return parent and its child node with the given child id or {@link Optional#empty()}.
     */
    Optional<Pair<ReconciliationEngine<EVENT>, EntityHolder>> findEngineByChildId(String childId);

    /**
     * Returns all roots of {@link ReconciliationEngine} instances ordered by the requested ordering criteria. The returned
     * list is immutable, and constitutes a snapshot of the entity model.
     *
     * @throws IllegalArgumentException if the ordering criteria are not recognized
     */
    <ORDER_BY> List<EntityHolder> orderedView(ORDER_BY orderingCriteria);

    /**
     * Creates a new reconciliation engine.
     */
    Observable<ReconciliationEngine<EVENT>> newEngine(EntityHolder bootstrapModel);

    /**
     * Removes an existing reconciliation engine.
     */
    Completable removeEngine(ReconciliationEngine<EVENT> engine);
}
