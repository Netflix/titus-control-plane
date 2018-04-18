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

import java.util.Optional;

import com.netflix.titus.common.framework.reconciler.ChangeAction;
import com.netflix.titus.common.framework.reconciler.EntityHolder;
import com.netflix.titus.common.framework.reconciler.ModelActionHolder;
import com.netflix.titus.common.framework.reconciler.ReconcileEventFactory;
import com.netflix.titus.common.framework.reconciler.ReconciliationEngine;

/**
 */
public class SimpleReconcilerEventFactory implements ReconcileEventFactory<SimpleReconcilerEvent> {

    @Override
    public SimpleReconcilerEvent newBeforeChangeEvent(ReconciliationEngine engine, ChangeAction changeAction, String transactionId) {
        return new SimpleReconcilerEvent(SimpleReconcilerEvent.EventType.ChangeRequest, changeAction.toString(), Optional.empty());
    }

    @Override
    public SimpleReconcilerEvent newAfterChangeEvent(ReconciliationEngine engine, ChangeAction changeAction, long executionTimeMs, String transactionId) {
        return new SimpleReconcilerEvent(SimpleReconcilerEvent.EventType.Changed, changeAction.toString(), Optional.empty());
    }

    @Override
    public SimpleReconcilerEvent newChangeErrorEvent(ReconciliationEngine engine, ChangeAction changeAction, Throwable error, long executionTimeMs, String transactionId) {
        return new SimpleReconcilerEvent(SimpleReconcilerEvent.EventType.Changed, changeAction.toString(), Optional.of(error));
    }

    @Override
    public SimpleReconcilerEvent newModelEvent(ReconciliationEngine engine, EntityHolder newRoot) {
        return new SimpleReconcilerEvent(SimpleReconcilerEvent.EventType.ModelInitial, "init", Optional.empty());
    }

    @Override
    public SimpleReconcilerEvent newModelUpdateEvent(ReconciliationEngine engine, ChangeAction changeAction, ModelActionHolder modelActionHolder, EntityHolder changedEntityHolder, Optional previousEntityHolder, String transactionId) {
        return new SimpleReconcilerEvent(SimpleReconcilerEvent.EventType.ModelUpdated, changedEntityHolder.getEntity(), Optional.empty());
    }

    @Override
    public SimpleReconcilerEvent newModelUpdateErrorEvent(ReconciliationEngine engine, ChangeAction changeAction, ModelActionHolder modelActionHolder, EntityHolder previousEntityHolder, Throwable error, String transactionId) {
        return new SimpleReconcilerEvent(SimpleReconcilerEvent.EventType.ModelUpdateError, previousEntityHolder.getEntity(), Optional.of(error));
    }
}
