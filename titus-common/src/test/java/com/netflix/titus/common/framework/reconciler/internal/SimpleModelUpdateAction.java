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

import com.google.common.base.Preconditions;
import com.netflix.titus.common.framework.reconciler.EntityHolder;
import com.netflix.titus.common.framework.reconciler.ModelAction;
import com.netflix.titus.common.util.tuple.Pair;

/**
 */
public class SimpleModelUpdateAction implements ModelAction {
    private final EntityHolder entityHolder;
    private final boolean isRoot;

    public SimpleModelUpdateAction(EntityHolder entityHolder, boolean isRoot) {
        Preconditions.checkArgument(entityHolder.getChildren().isEmpty(), "EntryHolder with no children expected");
        this.entityHolder = entityHolder;
        this.isRoot = isRoot;
    }

    @Override
    public Optional<Pair<EntityHolder, EntityHolder>> apply(EntityHolder rootHolder) {
        if (isRoot) {
            EntityHolder newRoot = rootHolder.setEntity(entityHolder.getEntity());
            return Optional.of(Pair.of(newRoot, newRoot));
        }
        EntityHolder newRoot = rootHolder.addChild(entityHolder);
        return Optional.of(Pair.of(newRoot, entityHolder));
    }
}
