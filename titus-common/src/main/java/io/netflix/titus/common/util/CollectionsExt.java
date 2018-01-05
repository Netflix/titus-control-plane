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

package io.netflix.titus.common.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * A set of additional collections related functions.
 */
public final class CollectionsExt {

    private CollectionsExt() {
    }

    public static <K, V> boolean isNullOrEmpty(Map<K, V> map) {
        return map == null || map.isEmpty();
    }

    public static <T> boolean isNullOrEmpty(Collection<T> collection) {
        return collection == null || collection.isEmpty();
    }

    public static <T> boolean isNullOrEmpty(T[] array) {
        return array == null || array.length == 0;
    }

    public static <T, C extends Collection<T>> void ifNotEmpty(C collection, Runnable runnable) {
        if (!isNullOrEmpty(collection)) {
            runnable.run();
        }
    }

    public static <T, C extends Collection<T>> void applyNotEmpty(C collection, Consumer<C> consumer) {
        if (!isNullOrEmpty(collection)) {
            consumer.accept(collection);
        }
    }

    public static <T> List<T> nonNull(List<T> collection) {
        return collection == null ? Collections.emptyList() : collection;
    }

    public static <T> Set<T> nonNull(Set<T> collection) {
        return collection == null ? Collections.emptySet() : collection;
    }

    public static <K, V> Map<K, V> nonNull(Map<K, V> map) {
        return map == null ? Collections.emptyMap() : map;
    }

    public static <T> T first(Collection<T> collection) {
        Iterator<T> it = collection.iterator();
        return it.hasNext() ? it.next() : null;
    }

    public static <T> T last(List<T> list) {
        return list.isEmpty() ? null : list.get(list.size() - 1);
    }

    public static <T> T last(Stream<T> stream) {
        return last(stream.collect(Collectors.toList()));
    }

    public static <T> T getOrDefault(T[] array, int idx, T defaultValue) {
        Preconditions.checkArgument(idx >= 0, "Index cannot be negative number");
        if (array == null || array.length <= idx) {
            return defaultValue;
        }
        return array[idx];
    }

    public static <T> void addAll(Collection<T> target, T[] source) {
        for (T v : source) {
            target.add(v);
        }
    }

    public static <T> List<T> asList(T[] source, int from) {
        return asList(source, from, source.length);
    }

    public static <T> List<T> asList(T[] source, int from, int to) {
        Preconditions.checkArgument(from >= 0, "Negative index value");
        Preconditions.checkArgument(from <= to, "Invalid range (from > to)");
        Preconditions.checkArgument(source.length >= to, "Index out of bound");

        List<T> result = new ArrayList<>();
        for (int i = from; i < to; i++) {
            result.add(source[i]);
        }
        return result;
    }

    public static <T> Set<T> take(Set<T> collection, int count) {
        return copy(collection, new HashSet<>(), count);
    }

    public static <T, C extends Collection<T>> C copy(C source, C destination, int count) {
        Iterator<T> it = source.iterator();
        for (int i = 0; i < count && it.hasNext(); i++) {
            destination.add(it.next());
        }
        return destination;
    }

    public static <T> List<T> copyAndAdd(List<T> original, T newItem) {
        List<T> newList = new ArrayList<>(original);
        newList.add(newItem);
        return newList;
    }

    public static <K, V> Map<K, V> copyAndAdd(Map<K, V> original, K key, V value) {
        if (original.isEmpty()) {
            return Collections.singletonMap(key, value);
        }
        Map<K, V> result = new HashMap<>(original);
        result.put(key, value);
        return result;
    }

    public static <T> List<T> nullableImmutableCopyOf(List<T> original) {
        return original == null ? null : ImmutableList.copyOf(original);
    }

    public static <K, V> Map<K, V> nullableImmutableCopyOf(Map<K, V> original) {
        return original == null ? null : ImmutableMap.copyOf(original);
    }

    public static <T> Set<T> copyAndRemove(Set<T> original, T toRemove) {
        if (original.isEmpty()) {
            return original;
        }
        if (!original.contains(toRemove)) {
            return original;
        }
        Set<T> result = new HashSet<>(original);
        result.remove(toRemove);
        return result;
    }

    public static <T> Set<T> copyAndRemove(Set<T> original, Collection<T> toRemove) {
        if (original.isEmpty()) {
            return Collections.emptySet();
        }
        Set<T> result = new HashSet<>(original);
        if (!toRemove.isEmpty()) {
            result.removeAll(toRemove);
        }
        return result;
    }

    public static <K, V> Map<K, V> copyAndRemove(Map<K, V> original, K... keys) {
        Map<K, V> result = new HashMap<>(original);
        for (K key : keys) {
            if (key != null) {
                result.remove(key);
            }
        }
        return result;
    }

    public static <K, V> Map<K, V> copyAndRemoveByValue(Map<K, V> original, Predicate<V> removePredicate) {
        return original.entrySet().stream()
                .filter(entry -> !removePredicate.test(entry.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @SafeVarargs
    public static <T> List<T> merge(List<T>... lists) {
        if (isNullOrEmpty(lists)) {
            return Collections.emptyList();
        }
        List<T> result = new ArrayList<>();
        for (List<T> next : lists) {
            result.addAll(next);
        }
        return result;
    }

    @SafeVarargs
    public static <T> Set<T> merge(Set<T>... sets) {
        if (isNullOrEmpty(sets)) {
            return Collections.emptySet();
        }
        Set<T> result = new HashSet<>();
        for (Set<T> next : sets) {
            result.addAll(next);
        }
        return result;
    }

    @SafeVarargs
    public static <K, V> Map<K, V> merge(Map<K, V>... maps) {
        if (isNullOrEmpty(maps)) {
            return Collections.emptyMap();
        }
        Map<K, V> result = new HashMap<>();
        for (Map<K, V> next : maps) {
            result.putAll(next);
        }
        return result;
    }

    @SafeVarargs
    public static <T> Set<T> asSet(T... values) {
        Set<T> newSet = new HashSet<>();
        Collections.addAll(newSet, values);
        return newSet;
    }

    public static <T> Set<T> asSet(T[] values, int from, int to) {
        Preconditions.checkArgument(from >= 0, "Invalid sub-sequence first position: %s", from);
        Preconditions.checkArgument(to >= from && to <= values.length, "Invalid sub-sequence last to position: %s", to);

        Set<T> newSet = new HashSet<>();
        for (int i = from; i < to; i++) {
            newSet.add(values[i]);
        }
        return newSet;
    }

    @SafeVarargs
    public static <T> Map<T, T> asMap(T... values) {
        Preconditions.checkArgument(values.length % 2 == 0, "Expected even number of arguments");
        Map<T, T> result = new HashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            result.put(values[i], values[i + 1]);
        }
        return result;
    }

    public static <K, V> Map<K, V> zipToMap(Collection<K> keys, Collection<V> values) {
        Preconditions.checkArgument(keys.size() == values.size(), "Expected collections of the same size");
        if (keys.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<K, V> result = new HashMap<>();
        Iterator<K> kIt = keys.iterator();
        Iterator<V> vIt = values.iterator();
        while (kIt.hasNext()) {
            result.put(kIt.next(), vIt.next());
        }
        return result;
    }

    public static <K, V> MapBuilder<K, V> newHashMap() {
        return new MapBuilder<>(new HashMap<>());
    }

    public static <K, V> MapBuilder<K, V> newHashMap(Map<K, V> original) {
        return new MapBuilder<>(new HashMap<>(original));
    }

    public static int[] toPrimitiveArray(Collection<Integer> collection) {
        int[] result = new int[collection.size()];
        Iterator<Integer> it = collection.iterator();
        for (int i = 0; it.hasNext(); i++) {
            result[i] = it.next();
        }
        return result;
    }

    public static char[] toPrimitiveCharArray(Collection<Character> collection) {
        char[] result = new char[collection.size()];
        Iterator<Character> it = collection.iterator();
        for (int i = 0; it.hasNext(); i++) {
            result[i] = it.next();
        }
        return result;
    }

    public static char[] toPrimitiveCharArray(Character[] arrayOfChar) {
        char[] result = new char[arrayOfChar.length];
        for (int i = 0; i < arrayOfChar.length; i++) {
            result[i] = arrayOfChar[i];
        }
        return result;
    }

    public static List<Integer> toWrapperList(int[] intArray) {
        List<Integer> result = new ArrayList<>();
        for (int v : intArray) {
            result.add(v);
        }
        return result;
    }

    public static <T> List<List<T>> chop(List<T> list, int chunkSize) {
        if (list.size() <= chunkSize) {
            return Collections.singletonList(list);
        }
        List<List<T>> result = new ArrayList<>();
        for (int i = 0; i < list.size(); i += chunkSize) {
            result.add(list.subList(i, Math.min(i + chunkSize, list.size())));
        }
        return result;
    }

    /**
     * {@link Optional#empty()} if the collection is <tt>null</tt> or {@link Collection#isEmpty() empty}.
     */
    public static <T, C extends Collection<T>> Optional<C> optionalOfNotEmpty(C collection) {
        if (isNullOrEmpty(collection)) {
            return Optional.empty();
        }
        return Optional.of(collection);
    }

    public static class MapBuilder<K, V> {

        private final Map<K, V> out;

        private MapBuilder(Map<K, V> out) {
            this.out = out;
        }

        public MapBuilder<K, V> entry(K key, V value) {
            out.put(key, value);
            return this;
        }

        public Map<K, V> toMap() {
            return out;
        }
    }
}
