/*
 * Copyright 2021 Netflix, Inc.
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
import java.util.Map;
import java.util.function.Predicate;

import com.netflix.titus.common.util.CollectionsExt;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;

class DefaultGroup<GROUP_KEY, PRIMARY_KEY, INPUT, OUTPUT> implements Group<GROUP_KEY, PRIMARY_KEY, OUTPUT> {

    private final IndexSpec<GROUP_KEY, PRIMARY_KEY, INPUT, OUTPUT> spec;
    private final PMap<PRIMARY_KEY, GROUP_KEY> primaryKeyToGroupKey;
    private final PMap<GROUP_KEY, PMap<PRIMARY_KEY, OUTPUT>> indexedValues;

    DefaultGroup(IndexSpec<GROUP_KEY, PRIMARY_KEY, INPUT, OUTPUT> spec,
                 PMap<PRIMARY_KEY, GROUP_KEY> primaryKeyToGroupKey,
                 PMap<GROUP_KEY, PMap<PRIMARY_KEY, OUTPUT>> indexedValues) {
        this.spec = spec;
        this.primaryKeyToGroupKey = primaryKeyToGroupKey;
        this.indexedValues = indexedValues;
    }

    static <GROUP_KEY, PRIMARY_KEY, INPUT, OUTPUT>
    DefaultGroup<GROUP_KEY, PRIMARY_KEY, INPUT, OUTPUT> newEmpty(IndexSpec<GROUP_KEY, PRIMARY_KEY, INPUT, OUTPUT> spec) {
        return new DefaultGroup<>(spec, HashTreePMap.empty(), HashTreePMap.empty());
    }

    @Override
    public Map<GROUP_KEY, Map<PRIMARY_KEY, OUTPUT>> get() {
        Map raw = indexedValues;
        return raw;
    }

    DefaultGroup<GROUP_KEY, PRIMARY_KEY, INPUT, OUTPUT> add(Collection<INPUT> values) {
        if (CollectionsExt.isNullOrEmpty(values)) {
            return this;
        }
        PMap<GROUP_KEY, PMap<PRIMARY_KEY, OUTPUT>> newIndexedValues = indexedValues;
        PMap<PRIMARY_KEY, GROUP_KEY> newPrimaryKeyToGroupKey = primaryKeyToGroupKey;
        for (INPUT value : values) {
            Predicate<INPUT> filter = spec.getFilter();
            if (!filter.test(value)) {
                continue;
            }
            GROUP_KEY indexKey = spec.getIndexKeyExtractor().apply(value);
            PRIMARY_KEY primaryKey = spec.getPrimaryKeyExtractor().apply(value);
            OUTPUT output = spec.getTransformer().apply(value);
            if (indexKey != null && primaryKey != null) {
                PMap<PRIMARY_KEY, OUTPUT> byIndexKey = newIndexedValues.get(indexKey);
                if (byIndexKey == null) {
                    newIndexedValues = newIndexedValues.plus(indexKey, HashTreePMap.singleton(primaryKey, output));
                } else {
                    newIndexedValues = newIndexedValues.plus(indexKey, byIndexKey.plus(primaryKey, output));
                }
                newPrimaryKeyToGroupKey = newPrimaryKeyToGroupKey.plus(primaryKey, indexKey);
            }
        }
        return new DefaultGroup<>(spec, newPrimaryKeyToGroupKey, newIndexedValues);
    }

    DefaultGroup<GROUP_KEY, PRIMARY_KEY, INPUT, OUTPUT> remove(Collection<PRIMARY_KEY> keys) {
        if (CollectionsExt.isNullOrEmpty(keys)) {
            return this;
        }
        PMap<GROUP_KEY, PMap<PRIMARY_KEY, OUTPUT>> newIndexedValues = indexedValues;
        PMap<PRIMARY_KEY, GROUP_KEY> newPrimaryKeyToGroupKey = primaryKeyToGroupKey;
        for (PRIMARY_KEY primaryKey : keys) {
            GROUP_KEY indexKey = primaryKeyToGroupKey.get(primaryKey);
            if (indexKey != null) {
                PMap<PRIMARY_KEY, OUTPUT> byIndexKey = newIndexedValues.get(indexKey);
                if (byIndexKey != null && byIndexKey.containsKey(primaryKey)) {
                    PMap<PRIMARY_KEY, OUTPUT> byPrimaryKey = byIndexKey.minus(primaryKey);
                    if (byPrimaryKey.isEmpty()) {
                        newIndexedValues = newIndexedValues.minus(indexKey);
                    } else {
                        newIndexedValues = newIndexedValues.plus(indexKey, byPrimaryKey);
                    }
                    newPrimaryKeyToGroupKey = newPrimaryKeyToGroupKey.minus(primaryKey);
                }
            }
        }
        // If nothing changed, return this the some object.
        if (newPrimaryKeyToGroupKey == primaryKeyToGroupKey) {
            return this;
        }
        return new DefaultGroup<>(spec, newPrimaryKeyToGroupKey, newIndexedValues);
    }
}
