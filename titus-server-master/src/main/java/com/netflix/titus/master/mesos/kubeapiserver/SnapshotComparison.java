/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.master.mesos.kubeapiserver;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.netflix.titus.common.util.CollectionsExt;

public class SnapshotComparison<T> {
    private final List<T> added;
    private final List<T> removed;
    private final List<T> modified;
    private final List<T> unmodified;

    private SnapshotComparison(List<T> added, List<T> removed, List<T> modified, List<T> unmodified) {
        this.added = added;
        this.removed = removed;
        this.modified = modified;
        this.unmodified = unmodified;
    }

    public static <T> SnapshotComparison<T> compare(Snapshot<T> first, Snapshot<T> second, Function<T, String> identityFn, BiPredicate<T, T> equalFn) {
        List<T> firstSnapshotItems = first.getItems();
        List<T> secondSnapshotItems = second.getItems();
        Map<String, T> firstItemsMap = firstSnapshotItems.stream().collect(Collectors.toMap(identityFn, Function.identity()));
        Map<String, T> secondItemsMap = secondSnapshotItems.stream().collect(Collectors.toMap(identityFn, Function.identity()));

        Set<String> firstSnapshotIds = firstItemsMap.keySet();
        Set<String> secondSnapshotIds = secondItemsMap.keySet();
        Set<String> addedIds = CollectionsExt.copyAndRemove(secondSnapshotIds, firstSnapshotIds);
        Set<String> removedIds = CollectionsExt.copyAndRemove(firstSnapshotIds, secondSnapshotIds);
        Set<String> sameIds = new HashSet<>(firstSnapshotIds);
        sameIds.retainAll(secondSnapshotIds);

        List<T> addedItems = addedIds.stream()
                .map(secondItemsMap::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        List<T> removedItems = removedIds.stream().
                map(firstItemsMap::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        List<T> modifiedItems = new ArrayList<>();
        List<T> unmodifiedItems = new ArrayList<>();

        for (String id : sameIds) {
            T firstItem = firstItemsMap.get(id);
            T secondItem = firstItemsMap.get(id);
            if (equalFn.test(firstItem, secondItem)) {
                unmodifiedItems.add(secondItem);
            } else {
                modifiedItems.add(secondItem);
            }
        }
        return new SnapshotComparison<>(addedItems, removedItems, modifiedItems, unmodifiedItems);
    }

    public List<T> getAdded() {
        return added;
    }

    public List<T> getRemoved() {
        return removed;
    }

    public List<T> getModified() {
        return modified;
    }

    public List<T> getUnmodified() {
        return unmodified;
    }
}
