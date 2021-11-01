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

import java.util.Optional;

/**
 * Each change in {@link ReconciliationEngine} emits a notification. Notification types are not predefined by the
 * framework, and instead a user must provide a factory to produce them. The events are emitted via
 * {@link ReconciliationEngine#events()}, and {@link ReconciliationFramework#events()} observables.
 *
 * @param <EVENT> event model type
 */
public interface ReconcileEventFactory<EVENT> {

    EVENT newCheckpointEvent(long timestamp);

    /**
     * Called when a new {@link ChangeAction} is registered, but not executed yet.
     */
    EVENT newBeforeChangeEvent(ReconciliationEngine<EVENT> engine,
                               ChangeAction changeAction,
                               String transactionId);

    /**
     * Called when a {@link ChangeAction} execution is completed.
     */
    EVENT newAfterChangeEvent(ReconciliationEngine<EVENT> engine,
                              ChangeAction changeAction,
                              long waitTimeMs,
                              long executionTimeMs,
                              String transactionId);

    /**
     * Called when a {@link ChangeAction} execution is completes with an error.
     */
    EVENT newChangeErrorEvent(ReconciliationEngine<EVENT> engine,
                              ChangeAction changeAction,
                              Throwable error,
                              long waitTimeMs,
                              long executionTimeMs,
                              String transactionId);

    /**
     * Called when a new {@link ReconciliationEngine} instance is created, and populated with the initial model.
     */
    EVENT newModelEvent(ReconciliationEngine<EVENT> engine, EntityHolder newRoot);

    /**
     * Called after each update to {@link EntityHolder} instance.
     */
    EVENT newModelUpdateEvent(ReconciliationEngine<EVENT> engine,
                              ChangeAction changeAction,
                              ModelActionHolder modelActionHolder,
                              EntityHolder changedEntityHolder,
                              Optional<EntityHolder> previousEntityHolder,
                              String transactionId);

    /**
     * Called after failed update to an {@link EntityHolder} instance.
     */
    EVENT newModelUpdateErrorEvent(ReconciliationEngine<EVENT> engine,
                                   ChangeAction changeAction,
                                   ModelActionHolder modelActionHolder,
                                   EntityHolder previousEntityHolder,
                                   Throwable error,
                                   String transactionId);
}
