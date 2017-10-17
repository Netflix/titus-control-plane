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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TODO Optimize. This is expensive and inefficient implementation.
 */
public class IndexSet<T> {

    private static final IndexSet<?> EMPTY = new IndexSet<>(Collections.emptyMap());

    private final Map<Object, Index<T>> indexes;

    private IndexSet(Map<Object, Index<T>> indexes) {
        this.indexes = indexes;
    }

    public IndexSet<T> apply(Collection<T> added) {
        Map<Object, Index<T>> copy = new HashMap<>();
        indexes.forEach((k, v) -> copy.put(k, v.apply(added)));
        return new IndexSet<>(copy);
    }

    public List<T> getOrdered(Object indexId) {
        Index<T> result = indexes.get(indexId);
        if (result == null) {
            throw new IllegalArgumentException("Unknown index id " + indexId);
        }
        return result.getOrdered();
    }

    public static <T> IndexSet<T> empty() {
        return (IndexSet<T>) EMPTY;
    }

    public static <T> IndexSet<T> newIndexSet(Map<Object, Comparator<T>> comparators) {
        Map<Object, Index<T>> indexes = new HashMap<>();
        comparators.forEach((k, v) -> indexes.put(k, Index.newIndex(v)));
        return new IndexSet<>(indexes);
    }

    static class Index<T> {

        private final Comparator<T> comparator;
        private final List<T> ordered;

        private Index(Comparator<T> comparator, List<T> ordered) {
            this.comparator = comparator;
            this.ordered = ordered;
        }

        Index<T> apply(Collection<T> added) {
            List<T> copy = new ArrayList<>(added);
            copy.sort(comparator);
            return new Index<>(comparator, copy);
        }

        List<T> getOrdered() {
            return ordered;
        }

        static <T> Index<T> newIndex(Comparator<T> comparator) {
            return new Index<>(comparator, Collections.emptyList());
        }
    }
}
