package io.netflix.titus.common.framework.reconciler;

import java.util.Optional;

public interface ReconcileEventFactory<CHANGE, EVENT> {

    EVENT newBeforeChangeEvent(ReconciliationEngine<CHANGE, EVENT> engine,
                               ChangeAction<CHANGE> changeAction,
                               long transactionId);

    EVENT newAfterChangeEvent(ReconciliationEngine<CHANGE, EVENT> engine,
                              ChangeAction<CHANGE> changeAction,
                              CHANGE change,
                              long executionTimeMs,
                              long transactionId);

    EVENT newChangeErrorEvent(ReconciliationEngine<CHANGE, EVENT> engine,
                              ChangeAction<CHANGE> changeAction,
                              Throwable error,
                              long executionTimeMs,
                              long transactionId);

    EVENT newModelEvent(ReconciliationEngine<CHANGE, EVENT> engine, EntityHolder newRoot);

    EVENT newModelUpdateEvent(ReconciliationEngine<CHANGE, EVENT> engine,
                              ChangeAction<CHANGE> changeAction,
                              ModelActionHolder modelActionHolder,
                              EntityHolder changedEntityHolder,
                              Optional<EntityHolder> previousEntityHolder,
                              long transactionId);

    EVENT newModelUpdateErrorEvent(ReconciliationEngine<CHANGE, EVENT> engine,
                                   ChangeAction<CHANGE> changeAction,
                                   ModelActionHolder modelActionHolder,
                                   EntityHolder previousEntityHolder,
                                   Throwable error,
                                   long transactionId);
}
