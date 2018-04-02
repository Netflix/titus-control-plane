/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.titus.common.util.rx.batch;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * @param <I> type of the batch identifier (index)
 * @param <T> type of items in the batch
 */
public class Batch<T extends Batchable<?>, I> {
    private static final Comparator<Batch<?, ?>> COMPARING_BY_SIZE = Comparator.comparingInt(b -> b.getItems().size());

    public static Comparator<Batch<?, ?>> bySize() {
        return COMPARING_BY_SIZE;
    }

    private final I index;
    private final List<T> items;
    private final Instant oldestItemTimestamp;

    @SafeVarargs
    public static <T extends Batchable<?>, I> Batch<T, I> of(I index, T... items) {
        return of(index, Arrays.asList(items));
    }

    public static <T extends Batchable<?>, I> Batch<T, I> of(I index, List<T> items) {
        return new Batch<>(index, items);
    }

    private Batch(I index, List<T> items) {
        this.index = index;
        this.items = Collections.unmodifiableList(items);
        // TODO: consider precomputing this as items are accumulated for a batch
        this.oldestItemTimestamp = items.stream().min(Comparator.comparing(Batchable::getTimestamp))
                .map(Batchable::getTimestamp)
                .orElse(Instant.now());
    }

    public I getIndex() {
        return index;
    }

    /**
     * @return an immutable {@link List}
     */
    public List<T> getItems() {
        return items;
    }

    public Instant getOldestItemTimestamp() {
        return oldestItemTimestamp;
    }

    public int size() {
        return items.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Batch)) {
            return false;
        }
        Batch<?, ?> batch = (Batch<?, ?>) o;
        return Objects.equals(getIndex(), batch.getIndex()) &&
                Objects.equals(getItems(), batch.getItems());
    }

    @Override
    public String toString() {
        return "Batch{" +
                "index=" + index +
                ", items=" + items +
                ", oldestItemTimestamp=" + oldestItemTimestamp +
                '}';
    }
}
