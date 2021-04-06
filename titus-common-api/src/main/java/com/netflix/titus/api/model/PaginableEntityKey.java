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

package com.netflix.titus.api.model;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * A key representation of an entity. Useful when the key is not the entity attribute and must be computed.
 */
public interface PaginableEntityKey<T> extends Comparable<PaginableEntityKey<T>> {

    T getEntity();

    static <T> List<T> sort(List<T> items, Function<T, PaginableEntityKey<T>> keyExtractor) {
        List<PaginableEntityKey<T>> keys = new ArrayList<>();
        for (T item : items) {
            keys.add(keyExtractor.apply(item));
        }
        keys.sort(Comparable::compareTo);
        List<T> sortedItems = new ArrayList<>();
        for (PaginableEntityKey<T> key : keys) {
            sortedItems.add(key.getEntity());
        }
        return sortedItems;
    }
}
