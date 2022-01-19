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

class DefaultIndexSet<PRIMARY_KEY, INPUT> implements IndexSet<PRIMARY_KEY, INPUT> {

    private final Map<String, DefaultGroup<?, PRIMARY_KEY, INPUT, ?>> groups;
    private final Map<String, DefaultIndex<?, PRIMARY_KEY, INPUT, ?>> indexes;
    private final Map<String, DefaultOrder<?, PRIMARY_KEY, INPUT, ?>> orders;

    DefaultIndexSet(Map<String, DefaultGroup<?, PRIMARY_KEY, INPUT, ?>> groups,
                    Map<String, DefaultIndex<?, PRIMARY_KEY, INPUT, ?>> indexes,
                    Map<String, DefaultOrder<?, PRIMARY_KEY, INPUT, ?>> orders) {
        this.groups = groups;
        this.indexes = indexes;
        this.orders = orders;
    }

    @Override
    public Map<String, Group<?, PRIMARY_KEY, ?>> getGroups() {
        return (Map) groups;
    }

    @Override
    public <INDEX_KEY, OUTPUT> Group<INDEX_KEY, PRIMARY_KEY, OUTPUT> getGroup(String groupId) {
        return (Group<INDEX_KEY, PRIMARY_KEY, OUTPUT>) groups.get(groupId);
    }

    @Override
    public Map<String, Index<?, ?>> getIndexes() {
        return (Map) indexes;
    }

    @Override
    public <UNIQUE_INDEX_KEY, OUTPUT> Index<UNIQUE_INDEX_KEY, OUTPUT> getIndex(String indexId) {
        return (Index<UNIQUE_INDEX_KEY, OUTPUT>) indexes.get(indexId);
    }

    @Override
    public Map<String, Order<?>> getOrders() {
        return (Map) orders;
    }

    @Override
    public <OUTPUT> Order<OUTPUT> getOrder(String orderId) {
        return (Order<OUTPUT>) orders.get(orderId);
    }

    @Override
    public IndexSet<PRIMARY_KEY, INPUT> add(Collection<INPUT> values) {
        Map<String, DefaultGroup<?, PRIMARY_KEY, INPUT, ?>> newGroups;
        if (groups.isEmpty()) {
            newGroups = Collections.emptyMap();
        } else {
            newGroups = new HashMap<>();
            groups.forEach((groupId, group) -> newGroups.put(groupId, group.add(values)));
        }

        Map<String, DefaultIndex<?, PRIMARY_KEY, INPUT, ?>> newIndexes;
        if (indexes.isEmpty()) {
            newIndexes = Collections.emptyMap();
        } else {
            newIndexes = new HashMap<>();
            indexes.forEach((indexId, index) -> newIndexes.put(indexId, index.add(values)));
        }

        Map<String, DefaultOrder<?, PRIMARY_KEY, INPUT, ?>> newOrders;
        if (orders.isEmpty()) {
            newOrders = Collections.emptyMap();
        } else {
            newOrders = new HashMap<>();
            orders.forEach((indexId, index) -> newOrders.put(indexId, index.add(values)));
        }

        return new DefaultIndexSet<>(newGroups, newIndexes, newOrders);
    }

    @Override
    public IndexSet<PRIMARY_KEY, INPUT> remove(Collection<PRIMARY_KEY> primaryKeys) {
        Map<String, DefaultGroup<?, PRIMARY_KEY, INPUT, ?>> newGroups;
        if (groups.isEmpty()) {
            newGroups = Collections.emptyMap();
        } else {
            newGroups = new HashMap<>();
            groups.forEach((groupId, group) -> newGroups.put(groupId, group.remove(primaryKeys)));
        }

        Map<String, DefaultIndex<?, PRIMARY_KEY, INPUT, ?>> newIndexes;
        if (indexes.isEmpty()) {
            newIndexes = Collections.emptyMap();
        } else {
            newIndexes = new HashMap<>();
            indexes.forEach((indexId, index) -> newIndexes.put(indexId, index.remove(primaryKeys)));
        }

        Map<String, DefaultOrder<?, PRIMARY_KEY, INPUT, ?>> newOrders;
        if (orders.isEmpty()) {
            newOrders = Collections.emptyMap();
        } else {
            newOrders = new HashMap<>();
            orders.forEach((indexId, index) -> newOrders.put(indexId, index.remove(primaryKeys)));
        }

        return new DefaultIndexSet<>(newGroups, newIndexes, newOrders);
    }
}
