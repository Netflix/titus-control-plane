package com.netflix.titus.common.framework.reconciler.internal;

import com.netflix.titus.common.framework.reconciler.ReconciliationEngine;

public interface InternalReconciliationEngine<EVENT>  extends ReconciliationEngine<EVENT> {

    boolean hasPendingTransactions();

    /**
     * Apply pending model updates. The model updates come from recently completed change actions (either requested or reconcile),
     * and must be processed by the event loop before next action(s) are started.
     *
     * @return true if the reference model (visible by the external clients) was updated
     */
    boolean applyModelUpdates();

    void emitEvents();

    boolean closeFinishedTransactions();

    /**
     * Emit events and execute pending actions. The execution order is:
     * <ul>
     * <li>Emit all queued events first</li>
     * <li>If there is pending reference change action, exit</li>
     * <li>Start next reference change action, if present and exit</li>
     * <li>If no reference action waits in the queue, check if there are running reconciliation actions. Exit if there are any.</li>
     * <li>Compute the difference between the reference and running states, and create reconcile action list</li>
     * <li>Start all independent actions from the beginning of the list</li>
     * </ul>
     *
     * @return true if there are actions running, false otherwise
     */
    boolean triggerActions();


}
