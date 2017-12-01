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

import java.util.List;

import io.netflix.titus.common.framework.reconciler.ChangeAction;
import io.netflix.titus.common.framework.reconciler.EntityHolder;
import io.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import io.netflix.titus.common.framework.reconciler.ReconciliationEngine.DifferenceResolver;

/**
 */
class ModelHolder<CHANGE, EVENT> {

    private final ReconciliationEngine<CHANGE, EVENT> engine;
    private final DifferenceResolver<CHANGE, EVENT> differenceResolver;

    private EntityHolder reference;
    private EntityHolder running;
    private EntityHolder store;

    ModelHolder(ReconciliationEngine<CHANGE, EVENT> engine, EntityHolder bootstrapModel, DifferenceResolver<CHANGE, EVENT> differenceResolver) {
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

    List<ChangeAction<CHANGE>> resolveDifference() {
        return differenceResolver.apply(engine);
    }
}
