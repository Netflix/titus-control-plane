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

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.PolledMeter;

/**
 * {@link IndexSet} metrics collector.
 */
class IndexSetMetrics implements Closeable {

    private final Registry registry;
    private final List<Id> ids;

    IndexSetMetrics(Id root, IndexSetHolder<?, ?> indexSetHolder, Registry registry) {
        this.registry = registry;

        List<Id> ids = new ArrayList<>();

        // Groups
        indexSetHolder.getIndexSet().getGroups().forEach((id, group) -> {
            Id metricId = root.withTags("kind", "group", "indexId", id);
            ids.add(metricId);
            PolledMeter.using(registry).withId(metricId).monitorValue(indexSetHolder, h -> {
                Group<?, ?, ?> g = h.getIndexSet().getGroup(id);
                return g != null ? g.get().size() : 0;
            });
        });

        // Indexes
        indexSetHolder.getIndexSet().getOrders().forEach((id, index) -> {
            Id metricId = root.withTags("kind", "index", "indexId", id);
            ids.add(metricId);
            PolledMeter.using(registry).withId(metricId).monitorValue(indexSetHolder, h -> {
                Order<Object> i = h.getIndexSet().getOrder(id);
                return i != null ? i.orderedList().size() : 0;
            });
        });

        this.ids = ids;
    }

    @Override
    public void close() {
        ids.forEach(id -> PolledMeter.remove(registry, id));
    }
}
