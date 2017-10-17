/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.common.framework.reconciler;

import java.util.List;

import rx.Observable;

/**
 */
public interface ReconciliationEngine<CHANGE> {

    interface DifferenceResolver<CHANGE> {
        List<ChangeAction<CHANGE>> apply(EntityHolder referenceModel, EntityHolder runningModel, EntityHolder storeModel);
    }

    class TriggerStatus {
        private final boolean runningChangeActions;
        private final boolean modelUpdates;

        public TriggerStatus(boolean runningChangeActions, boolean modelUpdates) {
            this.runningChangeActions = runningChangeActions;
            this.modelUpdates = modelUpdates;
        }

        public boolean isRunningChangeActions() {
            return runningChangeActions;
        }

        public boolean hasModelUpdates() {
            return modelUpdates;
        }
    }

    /**
     * Execute pending actions. The execution order is:
     * <ul>
     * <li>Apply state changes in the running model</li>
     * <li>If there is pending reference change action, do exit</li>
     * <li>Start next reference change action, if present and exit</li>
     * <li>If no reference action waits in the queue, check if there are running reconciliation actions. Exit if there are any.</li>
     * <li>Compute the difference between the reference and running states, and create reconcile action list</li>
     * <li>Start all independent actions from the beginning of the list</li>
     * </ul>
     *
     * @return true if there are actions running, false otherwise
     */
    TriggerStatus triggerEvents();

    /**
     * Change reference entity. The return observable completes successfully if the reference update was
     * successful. The action itself may include calls to external system to make the change persistent.
     * Examples of actions:
     * <ul>
     * <li>Job scale up</li>
     * <li>User requested task termination</li>
     * </ul>
     * Task completion can take some time, but it is always guarded by a timeout. If timeout triggers, the result is unknown.
     * Multiple change requests are processed in order of arrival, one at a time. If action execution deadline is
     * crossed, it is rejected. The deadline value must be always greater than the execution timeout.
     */
    Observable<Void> changeReferenceModel(ChangeAction<CHANGE> changeAction);

    /**
     * Returns immutable reference model.
     */
    EntityHolder getReferenceView();

    /**
     * Returns immutable running model.
     */
    EntityHolder getRunningView();

    /**
     * Returns immutable persisted model.
     */
    EntityHolder getStoreView();

    <ORDER_BY> List<EntityHolder> orderedView(ORDER_BY orderingCriteria);

    /**
     * Emits an event for each requested system change , and reconciliation action.
     */
    Observable<ReconcilerEvent> events();
}
