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

import java.util.List;

import com.netflix.titus.common.framework.reconciler.ChangeAction;
import com.netflix.titus.common.framework.reconciler.EntityHolder;
import com.netflix.titus.common.framework.reconciler.ReconciliationEngine;

/**
 */
class ModelHolder<EVENT> {

    private final ReconciliationEngine<EVENT> engine;
    private final ReconciliationEngine.DifferenceResolver<EVENT> differenceResolver;

    private EntityHolder reference;
    private EntityHolder running;
    private EntityHolder store;

    ModelHolder(ReconciliationEngine<EVENT> engine, EntityHolder bootstrapModel, ReconciliationEngine.DifferenceResolver<EVENT> differenceResolver) {
        this.engine = engine;
        this.differenceResolver = differenceResolver;
        this.reference = bootstrapModel;
        this.store = bootstrapModel;
        this.running = bootstrapModel;
    }

    EntityHolder getReference() {
        return reference;
    }

    EntityHolder getStore() {
        return store;
    }

    EntityHolder getRunning() {
        return running;
    }

    void setReference(EntityHolder reference) {
        this.reference = reference;
    }

    void setRunning(EntityHolder running) {
        this.running = running;
    }

    void setStore(EntityHolder store) {
        this.store = store;
    }

    List<ChangeAction> resolveDifference() {
        return differenceResolver.apply(engine);
    }
}
