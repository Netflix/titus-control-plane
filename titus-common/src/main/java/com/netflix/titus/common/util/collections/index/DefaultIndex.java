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
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import com.netflix.titus.common.util.CollectionsExt;
import org.pcollections.HashTreePMap;
import org.pcollections.HashTreePSet;
import org.pcollections.PMap;
import org.pcollections.PSet;

public class DefaultIndex<UNIQUE_INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> implements Index<UNIQUE_INDEX_KEY, OUTPUT> {

    private final IndexSpec<UNIQUE_INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> spec;
    private final PMap<PRIMARY_KEY, PSet<UNIQUE_INDEX_KEY>> primaryKeyToIndexKeys;
    private final PMap<UNIQUE_INDEX_KEY, OUTPUT> indexedValues;

    DefaultIndex(IndexSpec<UNIQUE_INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> spec,
                 PMap<PRIMARY_KEY, PSet<UNIQUE_INDEX_KEY>> primaryKeyToIndexKeys,
                 PMap<UNIQUE_INDEX_KEY, OUTPUT> indexedValues) {
        this.spec = spec;
        this.primaryKeyToIndexKeys = primaryKeyToIndexKeys;
        this.indexedValues = indexedValues;
    }

    static <UNIQUE_INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT>
    DefaultIndex<UNIQUE_INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> newEmpty(IndexSpec<UNIQUE_INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> spec) {
        return new DefaultIndex<>(spec, HashTreePMap.empty(), HashTreePMap.empty());
    }

    @Override
    public Map<UNIQUE_INDEX_KEY, OUTPUT> get() {
        return indexedValues;
    }

    DefaultIndex<UNIQUE_INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> add(Collection<INPUT> values) {
        if (CollectionsExt.isNullOrEmpty(values)) {
            return this;
        }
        PMap<PRIMARY_KEY, PSet<UNIQUE_INDEX_KEY>> newPrimaryKeyToIndexKeys = primaryKeyToIndexKeys;
        PMap<UNIQUE_INDEX_KEY, OUTPUT> newIndexedValues = indexedValues;
        for (INPUT value : values) {
            Predicate<INPUT> filter = spec.getFilter();
            if (!filter.test(value)) {
                continue;
            }

            Set<UNIQUE_INDEX_KEY> indexKeys = spec.getIndexKeysExtractor().apply(value);
            PRIMARY_KEY primaryKey = spec.getPrimaryKeyExtractor().apply(value);
            if (primaryKey != null) {
                PSet<UNIQUE_INDEX_KEY> currentKeys = primaryKeyToIndexKeys.get(primaryKey);
                if (CollectionsExt.isNullOrEmpty(indexKeys)) {
                    if (!CollectionsExt.isNullOrEmpty(currentKeys)) {
                        newPrimaryKeyToIndexKeys = newPrimaryKeyToIndexKeys.plus(primaryKey, HashTreePSet.empty());
                        newIndexedValues = newIndexedValues.minusAll(currentKeys);
                    }
                } else {
                    newPrimaryKeyToIndexKeys = newPrimaryKeyToIndexKeys.plus(primaryKey, HashTreePSet.from(indexKeys));
                    for (UNIQUE_INDEX_KEY key : indexKeys) {
                        newIndexedValues = newIndexedValues.plus(key, spec.getTransformer().apply(key, value));
                    }
                    if (!CollectionsExt.isNullOrEmpty(currentKeys)) {
                        for (UNIQUE_INDEX_KEY key : currentKeys) {
                            if (!indexKeys.contains(key)) {
                                newIndexedValues = newIndexedValues.minus(key);
                            }
                        }
                    }
                }
            }
        }

        return new DefaultIndex<>(spec, newPrimaryKeyToIndexKeys, newIndexedValues);
    }

    DefaultIndex<UNIQUE_INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> remove(Collection<PRIMARY_KEY> keys) {
        if (CollectionsExt.isNullOrEmpty(keys)) {
            return this;
        }
        PMap<PRIMARY_KEY, PSet<UNIQUE_INDEX_KEY>> newPrimaryKeyToIndexKeys = primaryKeyToIndexKeys;
        PMap<UNIQUE_INDEX_KEY, OUTPUT> newIndexedValues = indexedValues;
        for (PRIMARY_KEY primaryKey : keys) {
            PSet<UNIQUE_INDEX_KEY> indexKeys = newPrimaryKeyToIndexKeys.get(primaryKey);
            newPrimaryKeyToIndexKeys = newPrimaryKeyToIndexKeys.minus(primaryKey);
            if (indexKeys != null) {
                newIndexedValues = newIndexedValues.minusAll(indexKeys);
            }
        }

        return new DefaultIndex<>(spec, newPrimaryKeyToIndexKeys, newIndexedValues);
    }
}
