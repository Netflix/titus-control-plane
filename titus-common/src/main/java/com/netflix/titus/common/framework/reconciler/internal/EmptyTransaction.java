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

class EmptyTransaction implements Transaction {

    static final Transaction EMPTY = new EmptyTransaction();

    @Override
    public void close() {
    }

    @Override
    public boolean isClosed() {
        return true;
    }

    @Override
    public Optional<ModelHolder> applyModelUpdates(ModelHolder modelHolder) {
        return Optional.empty();
    }

    @Override
    public void emitEvents() {
    }

    @Override
    public boolean completeSubscribers() {
        return false;
    }
}
