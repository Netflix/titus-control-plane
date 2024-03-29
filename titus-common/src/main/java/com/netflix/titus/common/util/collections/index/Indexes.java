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

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;

public final class Indexes {

    private Indexes() {
    }

    public static <PRIMARY_KEY, INPUT> Builder<PRIMARY_KEY, INPUT> newBuilder() {
        return new Builder<>();
    }

    public static <PRIMARY_KEY, INPUT> IndexSet<PRIMARY_KEY, INPUT> empty() {
        return new Builder<PRIMARY_KEY, INPUT>().build();
    }

    public static Closeable monitor(Id id, IndexSetHolder<?, ?> indexSetHolder, Registry registry) {
        return new IndexSetMetrics(id, indexSetHolder, registry);
    }

    public static class Builder<PRIMARY_KEY, INPUT> {

        private final Map<String, DefaultGroup<?, PRIMARY_KEY, INPUT, ?>> groups = new HashMap<>();
        private final Map<String, DefaultIndex<?, PRIMARY_KEY, INPUT, ?>> indexes = new HashMap<>();
        private final Map<String, DefaultOrder<?, PRIMARY_KEY, INPUT, ?>> orders = new HashMap<>();

        public <INDEX_KEY, OUTPUT> Builder<PRIMARY_KEY, INPUT> withGroup(String groupId,
                                                                         IndexSpec<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> indexSpec) {
            groups.put(groupId, DefaultGroup.newEmpty(indexSpec));
            return this;
        }

        public <INDEX_KEY, OUTPUT> Builder<PRIMARY_KEY, INPUT> withIndex(String indexId,
                                                                         IndexSpec<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> indexSpec) {
            indexes.put(indexId, DefaultIndex.newEmpty(indexSpec));
            return this;
        }

        public <INDEX_KEY, OUTPUT> Builder<PRIMARY_KEY, INPUT> withOrder(String orderId,
                                                                         IndexSpec<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> indexSpec) {
            orders.put(orderId, DefaultOrder.newEmpty(indexSpec));
            return this;
        }

        public IndexSet<PRIMARY_KEY, INPUT> build() {
            return new DefaultIndexSet<>(groups, indexes, orders);
        }
    }
}
