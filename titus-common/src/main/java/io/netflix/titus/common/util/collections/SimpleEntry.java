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

import java.util.Map;

public final class SimpleEntry<K, V> implements Map.Entry<K, V> {
    private final K key;
    private V value;

    private SimpleEntry(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public static <K, V> Map.Entry<K, V> of(K key, V value) {
        return new SimpleEntry<>(key, value);
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public V setValue(V value) {
        V old = this.value;
        this.value = value;
        return old;
    }

    /**
     * @param o an instance of <tt>Map.Entry</tt>
     * @return true when keys and values of both <tt>Map.Entry</tt> match
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof Map.Entry)) {
            return false;
        }

        Map.Entry<?, ?> that = (Map.Entry<?, ?>) o;

        return (key != null ? key.equals(that.getKey()) : that.getKey() == null)
                && (value != null ? value.equals(that.getValue()) : that.getValue() == null);
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }
}
