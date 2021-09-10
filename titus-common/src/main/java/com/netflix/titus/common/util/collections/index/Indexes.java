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

import java.util.HashMap;
import java.util.Map;

public final class Indexes {

    private Indexes() {
    }

    public static <PRIMARY_KEY, INPUT, OUTPUT> Builder<PRIMARY_KEY, INPUT, OUTPUT> newBuilder() {
        return new Builder<>();
    }

    public static class Builder<PRIMARY_KEY, INPUT, OUTPUT> {

        private final Map<String, DefaultIndex<?, PRIMARY_KEY, INPUT, OUTPUT>> indexes = new HashMap<>();
        private final Map<String, DefaultGroup<?, PRIMARY_KEY, INPUT, OUTPUT>> groups = new HashMap<>();

        public <INDEX_KEY> Builder<PRIMARY_KEY, INPUT, OUTPUT> withIndex(String indexId,
                                                                         IndexSpec<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> indexSpec) {
            indexes.put(indexId, DefaultIndex.newEmpty(indexSpec));
            return this;
        }

        public <INDEX_KEY> Builder<PRIMARY_KEY, INPUT, OUTPUT> withGroup(String indexId,
                                                                         IndexSpec<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> indexSpec) {
            groups.put(indexId, DefaultGroup.newEmpty(indexSpec));
            return this;
        }

        public IndexSet<PRIMARY_KEY, INPUT, OUTPUT> build() {
            return new DefaultIndexSet<>(groups, indexes);
        }
    }
}
