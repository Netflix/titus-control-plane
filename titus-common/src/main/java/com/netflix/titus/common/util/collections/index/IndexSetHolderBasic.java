/*
 * Copyright 2022 Netflix, Inc.
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

package com.netflix.titus.common.util.collections.index;

import java.util.Collection;

/**
 * Basic, not thread safe implementation of {@link IndexSetHolder}.
 */
public class IndexSetHolderBasic<PRIMARY_KEY, INPUT> implements IndexSetHolder<PRIMARY_KEY, INPUT> {

    private volatile IndexSet<PRIMARY_KEY, INPUT> indexSet;

    public IndexSetHolderBasic(IndexSet<PRIMARY_KEY, INPUT> indexSet) {
        this.indexSet = indexSet;
    }

    @Override
    public IndexSet<PRIMARY_KEY, INPUT> getIndexSet() {
        return indexSet;
    }

    @Override
    public void add(Collection<INPUT> values) {
        this.indexSet = indexSet.add(values);
    }

    @Override
    public void remove(Collection<PRIMARY_KEY> values) {
        this.indexSet = indexSet.remove(values);
    }
}
