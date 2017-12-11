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

package io.netflix.titus.common.util.collections;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import io.netflix.titus.common.util.CollectionsExt;

/**
 * A concurrent (thread safe) Map that allows multiple unique values per key, with pluggable logic for determining
 * uniqueness and resolving conflicts.
 * <p>
 * Multiple values per key are kept as a <tt>HashMap</tt>, and determining uniqueness for each key is defined by a
 * pluggable <tt>Function</tt> that extracts unique identifiers for each value. Conflicts on a key are resolved by a
 * pluggable <tt>ConflictResolver</tt> as well.
 * <p>
 * This is inspired by Guava's HashMultiMap, and can be seen as a concurrent (thread safe) and lock-free implementation
 * of it. Writes use optimistic concurrency control and may be retried until succeeded. Reads are done without any
 * locking and return a point in time snapshot for each key. Modifications in each key are atomic and implemented with
 * copy-on-write of the current value.
 *
 * @param <K> type of keys. They must have a correct implementations of <tt>equals()</tt> and <tt>hashCode()</tt>
 * @param <V> type of values. Operations on values will use <tt>equals()</tt>, so it is hightly recommended it is properly implemented
 */
@ParametersAreNonnullByDefault
public class ConcurrentHashMultimap<K, V> implements Multimap<K, V> {
    private final ConcurrentMap<K, Map<Object, V>> entries = new ConcurrentHashMap<>();
    private final ValueIdentityExtractor<V> valueIdentityExtractor;
    private final ConflictResolver<V> defaultConflictResolver;

    public ConcurrentHashMultimap(ValueIdentityExtractor<V> valueIdentityExtractor, ConflictResolver<V> conflictResolver) {
        this.valueIdentityExtractor = valueIdentityExtractor;
        this.defaultConflictResolver = conflictResolver;
    }

    /**
     * Atomically adds a value for a key. Conflict resolution happens through a ConflictResolver if another existing
     * value with the same identity already exists.
     *
     * @param key   must have a reasonable implementation of equals() and hashCode()
     * @param value to be added, that can be uniquely identified by a ValueIdentityExtractor
     * @return true if the Map was updated, false if the existing value was kept, as per conflict resolution (ConflictResolver).
     */
    @Override
    public boolean put(@Nullable K key, @Nullable V value) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(value);
        return putWithConflictResolution(key, value, defaultConflictResolver);
    }

    private boolean putWithConflictResolution(K key, V newValue, ConflictResolver<V> conflictResolver) {
        // this can be updated multiple times by the compute call below. The last write wins. Never rely on its default value.
        final AtomicBoolean modified = new AtomicBoolean(false);
        final Object id = valueIdentityExtractor.apply(newValue);

        entries.compute(key, (ignored, values) -> {
            if (values == null) {
                values = new HashMap<>();
            }
            if (!values.containsKey(id)) {
                modified.set(true);
                return CollectionsExt.copyAndAdd(values, id, newValue);
            }

            // conflict detected
            if (conflictResolver.shouldReplace(values.get(id), newValue)) {
                modified.set(true);
                return CollectionsExt.copyAndAdd(values, id, newValue);
            }
            modified.set(false);
            return values;

        });
        return modified.get();
    }

    /**
     * Atomically removes a value from a key, based on its unique identity (provided by <tt>ValueIdentityExtractor</tt>).
     *
     * @param key   must be of a type compatible with the type defined for the <tt>MultiMap</tt>
     * @param value must be of a value comaptible with the type defined for the <tt>MultiMap</tt>
     * @return if the <tt>MultiMap</tt> changed
     * @throws ClassCastException if the key or value Objects are not appropriate for this <tt>MultiMap</tt>.
     */
    @Override
    public boolean remove(@Nullable Object key, @Nullable Object value) {
        K keyTyped = (K) key;
        V valueTyped = (V) value;
        return key != null && value != null && removeIf(keyTyped, valueTyped, v -> true);
    }

    /**
     * Atomically removes a value from a key, based on its unique identity (provided by <tt>ValueIdentityExtractor</tt>).
     * In cases where this <tt>Map</tt> is being updated with different items for the same identity, the third <tt>Predicate</tt>
     * param can be used to determine if the current value associated with the unique identity is the one to be removed.
     * <p>
     * This allows atomic updates in a <tt>compareAndDelete</tt> style to avoid race conditions.
     *
     * @param key   containing the value to be removed
     * @param value to be removed from the key
     * @return true if the <tt>MultiMap</tt> changed
     */
    public boolean removeIf(K key, V value, Predicate<V> match) {
        // this can be updated multiple times by the compute call below. The last write wins. Never rely on its default value.
        final AtomicBoolean modified = new AtomicBoolean(false);
        final Object id = valueIdentityExtractor.apply(value);

        entries.compute(key, (ignored, values) -> {
            if (values == null) {
                modified.set(false);
                return null;
            }
            if (!values.containsKey(id)) {
                modified.set(false);
                return values;
            }
            V currentValue = values.get(id);
            if (!match.test(currentValue)) {
                modified.set(false);
                return values;
            }
            modified.set(true);
            Map<Object, V> newValues = CollectionsExt.copyAndRemove(values, id);
            if (newValues.isEmpty()) {
                return null;
            }
            return newValues;
        });
        return modified.get();
    }

    /**
     * This is thread safe, but not atomic. It is equivalent to calling <tt>put</tt> for each value.
     *
     * @return if the <tt>MultiMap</tt> was modified
     */
    @Override
    public boolean putAll(@Nullable K key, Iterable<? extends V> values) {
        Preconditions.checkNotNull(key);
        return StreamSupport.stream(values.spliterator(), false)
                .map(value -> put(key, value))
                .reduce(false, (acc, modified) -> acc || modified);
    }

    /**
     * This is thread safe, but not atomic. It is equivalent to calling <tt>putAll</tt> for each key in the provided
     * <tt>MultiMap</tt>.
     *
     * @param values to be added
     * @return if the <tt>MultiMap</tt> was modified
     */
    @Override
    public boolean putAll(Multimap<? extends K, ? extends V> values) {
        return values.asMap().entrySet().stream()
                .map(entry -> putAll(entry.getKey(), entry.getValue()))
                .reduce(false, (acc, modified) -> acc || modified);
    }

    /**
     * Replace values (based on their identity provided by <tt>ValueIdentityExtractor</tt>) ignoring the conflict
     * resolution logic. This can be used to force replace some items associated with a key.
     * <p>
     * The entire operation is not atomic, and is almost equivalent to multiple calls to <tt>put()</tt>. The returned
     * <tt>Collection</tt> must be ignored, since it will not contain which items were replaced.
     * <p>
     * TODO: track replaced items
     *
     * @return this breaks the contract of the MultiMap interface and does not return the modified items
     */
    @Override
    public Collection<V> replaceValues(@Nullable K key, Iterable<? extends V> values) {
        Preconditions.checkNotNull(key);
        values.forEach(v -> putWithConflictResolution(key, v, (e, n) -> true));
        return ImmutableList.of();
    }

    /**
     * Atomically removes all values associated with a key.
     *
     * @param key must be compatible with the type defined for this MultiMap
     * @return the removed values
     * @throws ClassCastException if the key is not compatible with this <tt>MultiMap</tt>
     */
    @Override
    public Collection<V> removeAll(@Nullable Object key) {
        if (key == null) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableCollection(entries.remove(key).values());
    }

    /**
     * Atomically removes all entries from the Map
     */
    @Override
    public void clear() {
        entries.clear();
    }

    @Override
    public Collection<V> get(@Nullable K key) {
        if (key == null) {
            return ImmutableList.of();
        }
        final Map<Object, V> entries = this.entries.get(key);
        if (entries == null) {
            return null;
        }
        return entries.values();
    }

    @Override
    public Set<K> keySet() {
        return entries.keySet();
    }

    /**
     * Copies contents into an immutable <tt>Multiset</tt> view.
     *
     * @return a copied view of all keys, with counts for each one of them
     */
    @Override
    public Multiset<K> keys() {
        final ImmutableMultiset.Builder<K> builder = ImmutableMultiset.builder();
        entries.forEach((key, values) -> builder.setCount(key, values.size()));
        return builder.build();
    }

    /**
     * Return a view of all values. Multiple values with the same identity (as provided by <tt>ValueIdentityExtractor</tt>
     * may be present if they are associated with multiple keys.
     *
     * @return a copied view of all values, modifications will not be reflected back
     */
    @Override
    public Collection<V> values() {
        return entryStream().map(Map.Entry::getValue).collect(Collectors.toList());
    }

    /**
     * Copy and transform into a <tt>Collection</tt> of entries. Note that for a <tt>MultiMap</tt>, the same key can
     * appear multiple times in the returned collection.
     *
     * @return a copy of this <tt>MultiMap</tt>, modifications will not be reflected back on this
     */
    @Override
    public Collection<Map.Entry<K, V>> entries() {
        return entryStream().collect(Collectors.toList());
    }

    /**
     * This scans all items and is O(N).
     *
     * @return the total number of values in the
     */
    @Override
    public int size() {
        return (int) entryStream().count();
    }

    private Stream<Map.Entry<K, V>> entryStream() {
        return entries.entrySet().stream()
                .flatMap(entry -> entry.getValue().values().stream()
                        .map(value -> SimpleEntry.of(entry.getKey(), value)));
    }

    @Override
    public boolean isEmpty() {
        return entries.isEmpty();
    }

    @Override
    public boolean containsKey(@Nullable Object key) {
        return key != null && entries.containsKey(key);
    }

    /**
     * Scans each key for a given value, based on its unique identity (provided by <tt>ValueIdentityExtractor</tt>)
     *
     * @param value must be compatible with this <tt>MultiMap</tt>
     * @return true if the value is found in at least one key
     * @throws ClassCastException when the value is not compatible
     */
    @Override
    public boolean containsValue(@Nullable Object value) {
        if (value == null) {
            return false;
        }
        final Object id = valueIdentityExtractor.apply((V) value);
        return entries.entrySet().stream().map(Map.Entry::getValue).anyMatch(values -> values.containsKey(id));
    }

    /**
     * @param key   must be compatible
     * @param value must be compatible
     * @return true if the key contains a value based on its unique identity (provided by <tt>ValueIdentityProvider</tt>)
     * @throws ClassCastException when the provided key or value are not compatible
     */
    @Override
    public boolean containsEntry(@Nullable Object key, @Nullable Object value) {
        final Object id = valueIdentityExtractor.apply((V) value);
        return entries.get(key).containsKey(id);
    }

    /**
     * The returned <tt>Map</tt> has the same concurrency semantics as <tt>java.util.concurrent.ConcurrentMap</tt>,
     * in particular iterators on the returned view will operate on a point in time snapshot and may or may not see
     * concurrent modifications.
     *
     * @return a read only (i.e.: <tt>Collections.unmodifiableMap()</tt>) view of the MultiMap as a <tt>java.util.Map</tt>.
     * Values for each key are also read only and changes on them will not be reflected in the source MultiMap.
     */
    @Override
    public Map<K, Collection<V>> asMap() {
        // this assumes the entries Map can not contain null values, which should be true for ConcurrentMap implementations
        return Maps.transformValues(entries, Map::values);
    }

    /**
     * <tt>java.util.function.Function</tt> with logic for extracting unique identifiers for each value. Uniqueness is
     * defined by <tt>equals()</tt> and <tt>hashCode()</tt> of the returned identifier <tt>Object</tt>.
     * <p>
     * Returned identifiers are heavily encouraged to be immutable, since they will be used as keys in a <tt>Map</tt>,
     * and modifications will not be reflected in Collections they are being held in.
     */
    public interface ValueIdentityExtractor<V> extends Function<V, Object> {
    }

    /**
     * Logic for conflict resolution when a new value would replace an existing one with the same identity.
     * Implementations must return true if the existing value must be replaced.
     *
     * @param <V> type of items that will conflict. Implementations will receive the old,new
     */
    public interface ConflictResolver<V> extends BiFunction<V, V, Boolean> {
        /**
         * @param existing    value to be replaced
         * @param replacement potential new value
         * @return true if the existing value should be replaced
         */
        boolean shouldReplace(V existing, V replacement);

        @Override
        default Boolean apply(V existing, V replacement) {
            return shouldReplace(existing, replacement);
        }
    }

}
