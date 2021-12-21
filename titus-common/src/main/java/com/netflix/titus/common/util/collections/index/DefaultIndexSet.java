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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class DefaultIndexSet<PRIMARY_KEY, INPUT, OUTPUT> implements IndexSet<PRIMARY_KEY, INPUT, OUTPUT> {

    private final Map<String, DefaultGroup<?, PRIMARY_KEY, INPUT, OUTPUT>> groups;
    private final Map<String, DefaultIndex<?, PRIMARY_KEY, INPUT, OUTPUT>> indexes;

    DefaultIndexSet(Map<String, DefaultGroup<?, PRIMARY_KEY, INPUT, OUTPUT>> groups,
                    Map<String, DefaultIndex<?, PRIMARY_KEY, INPUT, OUTPUT>> indexes) {
        this.groups = groups;
        this.indexes = indexes;
    }

    @Override
    public <INDEX_KEY> Group<INDEX_KEY, PRIMARY_KEY, OUTPUT> getGroup(String groupId) {
        return (Group<INDEX_KEY, PRIMARY_KEY, OUTPUT>) groups.get(groupId);
    }

    @Override
    public Index<OUTPUT> getIndex(String indexId) {
        return indexes.get(indexId);
    }

    @Override
    public IndexSet<PRIMARY_KEY, INPUT, OUTPUT> add(Collection<INPUT> values) {
        Map<String, DefaultGroup<?, PRIMARY_KEY, INPUT, OUTPUT>> newGroups;
        if (groups.isEmpty()) {
            newGroups = Collections.emptyMap();
        } else {
            newGroups = new HashMap<>();
            groups.forEach((groupId, group) -> newGroups.put(groupId, group.add(values)));
        }

        Map<String, DefaultIndex<?, PRIMARY_KEY, INPUT, OUTPUT>> newIndexes;
        if (indexes.isEmpty()) {
            newIndexes = Collections.emptyMap();
        } else {
            newIndexes = new HashMap<>();
            indexes.forEach((indexId, index) -> newIndexes.put(indexId, index.add(values)));
        }

        return new DefaultIndexSet<>(newGroups, newIndexes);
    }

    @Override
    public IndexSet<PRIMARY_KEY, INPUT, OUTPUT> remove(Collection<PRIMARY_KEY> primaryKeys) {
        Map<String, DefaultGroup<?, PRIMARY_KEY, INPUT, OUTPUT>> newGroups;
        if (groups.isEmpty()) {
            newGroups = Collections.emptyMap();
        } else {
            newGroups = new HashMap<>();
            groups.forEach((groupId, group) -> newGroups.put(groupId, group.remove(primaryKeys)));
        }

        Map<String, DefaultIndex<?, PRIMARY_KEY, INPUT, OUTPUT>> newIndexes;
        if (indexes.isEmpty()) {
            newIndexes = Collections.emptyMap();
        } else {
            newIndexes = new HashMap<>();
            indexes.forEach((indexId, index) -> newIndexes.put(indexId, index.remove(primaryKeys)));
        }

        return new DefaultIndexSet<>(newGroups, newIndexes);
    }
}