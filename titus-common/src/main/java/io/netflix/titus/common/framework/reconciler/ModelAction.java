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

import java.util.Optional;

import io.netflix.titus.common.util.tuple.Pair;

public interface ModelAction {

    /**
     * Update an {@link EntityHolder} hierarchy. It is expected that only one {@link EntityHolder} is modified by
     * single action. The result always contains a new root value (left), and the entity that was created or updated (right).
     * If the root value was updated, both left and right fields contain a reference to the new root value.
     *
     * @return {@link Optional#empty()} if no change was made, or new versions of the {@link EntityHolder} entities
     */
    Optional<Pair<EntityHolder, EntityHolder>> apply(EntityHolder rootHolder);
}
