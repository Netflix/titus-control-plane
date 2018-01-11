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

package io.netflix.titus.common.framework.reconciler.internal;

import java.util.Optional;

import io.netflix.titus.common.framework.reconciler.ChangeAction;
import io.netflix.titus.common.framework.reconciler.EntityHolder;
import io.netflix.titus.common.framework.reconciler.ModelActionHolder;
import io.netflix.titus.common.framework.reconciler.ReconcileEventFactory;
import io.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import io.netflix.titus.common.framework.reconciler.internal.SimpleReconcilerEvent.EventType;

/**
 */
public class SimpleReconcilerEventFactory implements ReconcileEventFactory<SimpleReconcilerEvent> {

    static SimpleReconcilerEventFactory INSTANCE = new SimpleReconcilerEventFactory();

    @Override
    public SimpleReconcilerEvent newBeforeChangeEvent(ReconciliationEngine engine, ChangeAction changeAction, long transactionId) {
        return new SimpleReconcilerEvent(EventType.ChangeRequest, changeAction.toString(), Optional.empty());
    }

    @Override
    public SimpleReconcilerEvent newAfterChangeEvent(ReconciliationEngine engine, ChangeAction changeAction, long executionTimeMs, long transactionId) {
        return new SimpleReconcilerEvent(EventType.Changed, changeAction.toString(), Optional.empty());
    }

    @Override
    public SimpleReconcilerEvent newChangeErrorEvent(ReconciliationEngine engine, ChangeAction changeAction, Throwable error, long executionTimeMs, long transactionId) {
        return new SimpleReconcilerEvent(EventType.Changed, changeAction.toString(), Optional.of(error));
    }

    @Override
    public SimpleReconcilerEvent newModelEvent(ReconciliationEngine engine, EntityHolder newRoot) {
        return new SimpleReconcilerEvent(EventType.ModelInitial, "init", Optional.empty());
    }

    @Override
    public SimpleReconcilerEvent newModelUpdateEvent(ReconciliationEngine engine, ChangeAction changeAction, ModelActionHolder modelActionHolder, EntityHolder changedEntityHolder, Optional previousEntityHolder, long transactionId) {
        return new SimpleReconcilerEvent(EventType.ModelUpdated, changedEntityHolder.getEntity(), Optional.empty());
    }

    @Override
    public SimpleReconcilerEvent newModelUpdateErrorEvent(ReconciliationEngine engine, ChangeAction changeAction, ModelActionHolder modelActionHolder, EntityHolder previousEntityHolder, Throwable error, long transactionId) {
        return new SimpleReconcilerEvent(EventType.ModelUpdateError, previousEntityHolder.getEntity(), Optional.of(error));
    }
}
