package io.netflix.titus.common.framework.reconciler;

import java.util.Optional;

/**
 * Each change in {@link ReconciliationEngine} emits a notification. Notification types are not predefined by the
 * framework, and instead a user must provide a factory to produce them. The events are emitted via
 * {@link ReconciliationEngine#events()}, and {@link ReconciliationFramework#events()} observables.
 *
 * @param <CHANGE> an entity describing the result of applying a change action
 * @param <EVENT> event model type
 */
public interface ReconcileEventFactory<CHANGE, EVENT> {

    /**
     * Called when a new {@link ChangeAction} is registered, but not executed yet.
     */
    EVENT newBeforeChangeEvent(ReconciliationEngine<CHANGE, EVENT> engine,
                               ChangeAction<CHANGE> changeAction,
                               long transactionId);

    /**
     * Called when a {@link ChangeAction} execution is completed.
     */
    EVENT newAfterChangeEvent(ReconciliationEngine<CHANGE, EVENT> engine,
                              ChangeAction<CHANGE> changeAction,
                              CHANGE change,
                              long executionTimeMs,
                              long transactionId);

    /**
     * Called when a {@link ChangeAction} execution is completes with an error.
     */
    EVENT newChangeErrorEvent(ReconciliationEngine<CHANGE, EVENT> engine,
                              ChangeAction<CHANGE> changeAction,
                              Throwable error,
                              long executionTimeMs,
                              long transactionId);

    /**
     * Called when a new {@link ReconciliationEngine} instance is created, and populated with the initial model.
     */
    EVENT newModelEvent(ReconciliationEngine<CHANGE, EVENT> engine, EntityHolder newRoot);

    /**
     * Called after each update to {@link EntityHolder} instance.
     */
    EVENT newModelUpdateEvent(ReconciliationEngine<CHANGE, EVENT> engine,
                              ChangeAction<CHANGE> changeAction,
                              ModelActionHolder modelActionHolder,
                              EntityHolder changedEntityHolder,
                              Optional<EntityHolder> previousEntityHolder,
                              long transactionId);

    /**
     * Called after failed update to an {@link EntityHolder} instance.
     */
    EVENT newModelUpdateErrorEvent(ReconciliationEngine<CHANGE, EVENT> engine,
                                   ChangeAction<CHANGE> changeAction,
                                   ModelActionHolder modelActionHolder,
                                   EntityHolder previousEntityHolder,
                                   Throwable error,
                                   long transactionId);
}
