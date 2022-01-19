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

import java.util.Comparator;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.util.FunctionExt;

/**
 * A descriptor for a single {@link Group} or {@link Order} instance.
 */
public class IndexSpec<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> {

    private final BiFunction<INDEX_KEY, INPUT, OUTPUT> transformer;
    private final Function<INPUT, INDEX_KEY> indexKeyExtractor;
    private final Function<INPUT, Set<INDEX_KEY>> indexKeysExtractor;
    private final Function<INPUT, PRIMARY_KEY> primaryKeyExtractor;
    private final Comparator<INDEX_KEY> indexKeyComparator;
    private final Comparator<PRIMARY_KEY> primaryKeyComparator;
    private final Predicate<INPUT> filter;

    IndexSpec(BiFunction<INDEX_KEY, INPUT, OUTPUT> transformer,
              Function<INPUT, INDEX_KEY> indexKeyExtractor,
              Function<INPUT, Set<INDEX_KEY>> indexKeysExtractor,
              Function<INPUT, PRIMARY_KEY> primaryKeyExtractor,
              Comparator<INDEX_KEY> indexKeyComparator,
              Comparator<PRIMARY_KEY> primaryKeyComparator,
              Predicate<INPUT> filter) {
        this.transformer = transformer;
        this.indexKeyExtractor = indexKeyExtractor;
        this.indexKeysExtractor = indexKeysExtractor;
        this.primaryKeyExtractor = primaryKeyExtractor;
        this.indexKeyComparator = indexKeyComparator;
        this.primaryKeyComparator = primaryKeyComparator;
        this.filter = filter;
    }

    public BiFunction<INDEX_KEY, INPUT, OUTPUT> getTransformer() {
        return transformer;
    }

    Function<INPUT, INDEX_KEY> getIndexKeyExtractor() {
        return indexKeyExtractor;
    }

    Function<INPUT, Set<INDEX_KEY>> getIndexKeysExtractor() {
        return indexKeysExtractor;
    }

    Function<INPUT, PRIMARY_KEY> getPrimaryKeyExtractor() {
        return primaryKeyExtractor;
    }

    public Comparator<INDEX_KEY> getIndexKeyComparator() {
        return indexKeyComparator;
    }

    public Comparator<PRIMARY_KEY> getPrimaryKeyComparator() {
        return primaryKeyComparator;
    }

    public Predicate<INPUT> getFilter() {
        return filter;
    }

    public static <INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> Builder<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> newBuilder() {
        return new Builder<>();
    }

    public static final class Builder<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> {

        private BiFunction<INDEX_KEY, INPUT, OUTPUT> transformer;
        private Function<INPUT, INDEX_KEY> indexKeyExtractor;
        private Function<INPUT, Set<INDEX_KEY>> indexKeysExtractor;
        private Function<INPUT, PRIMARY_KEY> primaryKeyExtractor;
        private Comparator<INDEX_KEY> indexKeyComparator;
        private Comparator<PRIMARY_KEY> primaryKeyComparator;
        private Predicate<INPUT> filter;

        private Builder() {
        }

        public Builder<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> withTransformer(BiFunction<INDEX_KEY, INPUT, OUTPUT> transformer) {
            this.transformer = transformer;
            return this;
        }

        public Builder<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> withIndexKeyExtractor(Function<INPUT, INDEX_KEY> indexKeyExtractor) {
            this.indexKeyExtractor = indexKeyExtractor;
            return this;
        }

        public Builder<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> withIndexKeysExtractor(Function<INPUT, Set<INDEX_KEY>> indexKeysExtractor) {
            this.indexKeysExtractor = indexKeysExtractor;
            return this;
        }

        public Builder<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> withPrimaryKeyExtractor(Function<INPUT, PRIMARY_KEY> primaryKeyExtractor) {
            this.primaryKeyExtractor = primaryKeyExtractor;
            return this;
        }

        public Builder<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> withIndexKeyComparator(Comparator<INDEX_KEY> indexKeyComparator) {
            this.indexKeyComparator = indexKeyComparator;
            return this;
        }

        public Builder<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> withPrimaryKeyComparator(Comparator<PRIMARY_KEY> primaryKeyComparator) {
            this.primaryKeyComparator = primaryKeyComparator;
            return this;
        }

        public Builder<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> withFilter(Predicate<INPUT> filter) {
            this.filter = filter;
            return this;
        }

        public IndexSpec<INDEX_KEY, PRIMARY_KEY, INPUT, OUTPUT> build() {
            Preconditions.checkState(indexKeyExtractor != null || indexKeysExtractor != null, "Both index key and index keys extractors not set");
            Preconditions.checkNotNull(primaryKeyExtractor, "Primary key extractor not set");
            Preconditions.checkNotNull(indexKeyComparator, "Index key comparator not set");
            Preconditions.checkNotNull(primaryKeyComparator, "Primary key comparator not set");
            if (transformer == null) {
                transformer = (key, input) -> (OUTPUT) input;
            }
            if (filter == null) {
                filter = FunctionExt.alwaysTrue();
            }
            return new IndexSpec<>(transformer, indexKeyExtractor, indexKeysExtractor, primaryKeyExtractor, indexKeyComparator, primaryKeyComparator, filter);
        }
    }
}
