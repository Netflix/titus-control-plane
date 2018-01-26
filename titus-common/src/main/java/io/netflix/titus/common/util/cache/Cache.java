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

package io.netflix.titus.common.util.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A generic cache interface based on the open source library <a href="https://github.com/ben-manes/caffeine">Caffeine</a>.
 *
 * @param <K> - the type of the keys in the cache
 * @param <V> - the type of the values in the cache
 */
@ThreadSafe
public interface Cache<K, V> {
    @CheckForNull
    V getIfPresent(@Nonnull Object key);

    @CheckForNull
    V get(@Nonnull K key, @Nonnull Function<? super K, ? extends V> mappingFunction);

    @Nonnull
    Map<K, V> getAllPresent(@Nonnull Iterable<?> keys);

    void put(@Nonnull K key, @Nonnull V value);

    void putAll(@Nonnull Map<? extends K, ? extends V> map);

    void invalidate(@Nonnull Object key);

    void invalidateAll(@Nonnull Iterable<?> keys);

    void invalidateAll();

    @Nonnegative
    long estimatedSize();

    @Nonnull
    ConcurrentMap<K, V> asMap();

    void cleanUp();
}
