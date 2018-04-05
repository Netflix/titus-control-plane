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
import java.util.function.Function;

import com.netflix.titus.common.framework.reconciler.ChangeAction;
import com.netflix.titus.common.framework.reconciler.EntityHolder;
import com.netflix.titus.common.framework.reconciler.ReconciliationEngine;

public class DispatchingDifferenceResolver<EVENT> implements ReconciliationEngine.DifferenceResolver<EVENT> {

    private final Function<EntityHolder, ReconciliationEngine.DifferenceResolver<EVENT>> dispatcherFun;

    public DispatchingDifferenceResolver(Function<EntityHolder, ReconciliationEngine.DifferenceResolver<EVENT>> dispatcherFun) {
        this.dispatcherFun = dispatcherFun;
    }

    @Override
    public List<ChangeAction> apply(ReconciliationEngine<EVENT> engine) {
        return dispatcherFun.apply(engine.getReferenceView()).apply(engine);
    }
}
