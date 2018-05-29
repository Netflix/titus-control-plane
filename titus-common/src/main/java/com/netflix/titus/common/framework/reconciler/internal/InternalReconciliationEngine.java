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
