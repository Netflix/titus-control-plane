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

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.netflix.titus.common.util.CollectionsExt;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;
import org.pcollections.PVector;
import org.pcollections.TreePVector;

public class DefaultOrder<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> implements Order<OUTPUT> {

    private final IndexSpec<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> spec;
    private final PMap<PRIMARY_KEY, INDEX_KEY> primaryKeyToIndexKey;
    private final PVector<ListItem<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT>> sorted;
    private final List<OUTPUT> result;

    private final ListItemComparator<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> listItemComparator;

    DefaultOrder(IndexSpec<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> spec,
                 ListItemComparator<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> listItemComparator,
                 PMap<PRIMARY_KEY, INDEX_KEY> primaryKeyToIndexKey,
                 PVector<ListItem<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT>> sorted) {
        this.spec = spec;
        this.listItemComparator = listItemComparator;
        this.primaryKeyToIndexKey = primaryKeyToIndexKey;
        this.sorted = sorted;
        this.result = new AbstractList<OUTPUT>() {
            @Override
            public OUTPUT get(int index) {
                ListItem<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> item = sorted.get(index);
                return item == null ? null : item.getOutput();
            }

            @Override
            public int size() {
                return sorted.size();
            }
        };
    }

    static <INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT>
    DefaultOrder<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> newEmpty(IndexSpec<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> spec) {
        return new DefaultOrder<>(spec, new ListItemComparator<>(spec), HashTreePMap.empty(), TreePVector.empty());
    }

    @Override
    public List<OUTPUT> orderedList() {
        return result;
    }

    DefaultOrder<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> add(Collection<INPUT> values) {
        List<INPUT> filtered = filter(values);
        if (CollectionsExt.isNullOrEmpty(filtered)) {
            return this;
        }

        PVector<ListItem<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT>> newSorted = sorted;
        PMap<PRIMARY_KEY, INDEX_KEY> newPrimaryKeyToIndexKey = primaryKeyToIndexKey;
        for (INPUT input : filtered) {
            INDEX_KEY indexKey = spec.getIndexKeyExtractor().apply(input);
            PRIMARY_KEY primaryKey = spec.getPrimaryKeyExtractor().apply(input);
            int pos = findKeyPosition(indexKey, primaryKey, newSorted);
            OUTPUT output = spec.getTransformer().apply(indexKey, input);
            ListItem<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> newItem = new ListItem<>(indexKey, primaryKey, input, output);
            if (pos >= 0) {
                // Override existing item.
                newSorted = newSorted.minus(pos).plus(pos, newItem);
            } else {
                int insertionPoint = -(pos + 1);
                newSorted = newSorted.plus(insertionPoint, newItem);
            }
            newPrimaryKeyToIndexKey = newPrimaryKeyToIndexKey.plus(primaryKey, indexKey);
        }

        return new DefaultOrder<>(spec, listItemComparator, newPrimaryKeyToIndexKey, newSorted);
    }

    DefaultOrder<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> remove(Collection<PRIMARY_KEY> primaryKeys) {
        if (CollectionsExt.isNullOrEmpty(primaryKeys)) {
            return this;
        }

        PVector<ListItem<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT>> newSorted = sorted;
        PMap<PRIMARY_KEY, INDEX_KEY> newPrimaryKeyToIndexKey = primaryKeyToIndexKey;
        for (PRIMARY_KEY primaryKey : primaryKeys) {
            INDEX_KEY indexKey = primaryKeyToIndexKey.get(primaryKey);
            if (indexKey != null) {
                int pos = findKeyPosition(indexKey, primaryKey, newSorted);
                if (pos >= 0) {
                    // Remove existing item.
                    newSorted = newSorted.minus(pos);
                    newPrimaryKeyToIndexKey = newPrimaryKeyToIndexKey.minus(primaryKey);
                }
            }
        }

        return new DefaultOrder<>(spec, listItemComparator, newPrimaryKeyToIndexKey, newSorted);
    }

    private int findKeyPosition(INDEX_KEY indexKey,
                                PRIMARY_KEY primaryKey,
                                PVector<ListItem<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT>> sorted) {
        return PCollectionUtil.binarySearch(sorted, new ListItem<>(indexKey, primaryKey, null, null), listItemComparator);
    }

    private List<INPUT> filter(Collection<INPUT> values) {
        if (CollectionsExt.isNullOrEmpty(values)) {
            return Collections.emptyList();
        }

        // Filter
        List<INPUT> filtered = new ArrayList<>();
        for (INPUT value : values) {
            if (spec.getFilter().test(value)) {
                filtered.add(value);
            }
        }
        return filtered;
    }

    private static class ListItem<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> {

        private final INDEX_KEY indexKey;
        private final PRIMARY_KEY primaryKey;
        private final INPUT input;
        private final OUTPUT output;

        private ListItem(INDEX_KEY indexKey, PRIMARY_KEY primaryKey, INPUT input, OUTPUT output) {
            this.indexKey = indexKey;
            this.primaryKey = primaryKey;
            this.input = input;
            this.output = output;
        }

        private INDEX_KEY getIndexKey() {
            return indexKey;
        }

        private PRIMARY_KEY getPrimaryKey() {
            return primaryKey;
        }

        private INPUT getInput() {
            return input;
        }

        private OUTPUT getOutput() {
            return output;
        }
    }

    private static class ListItemComparator<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> implements Comparator<ListItem<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT>> {

        private final IndexSpec<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> spec;

        public ListItemComparator(IndexSpec<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> spec) {
            this.spec = spec;
        }

        @Override
        public int compare(ListItem<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> o1, ListItem<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> o2) {
            if (o1 == o2) {
                return 0;
            }
            int r = spec.getIndexKeyComparator().compare(o1.getIndexKey(), o2.getIndexKey());
            if (r != 0) {
                return r;
            }
            return spec.getPrimaryKeyComparator().compare(o1.getPrimaryKey(), o2.getPrimaryKey());
        }
    }
}
