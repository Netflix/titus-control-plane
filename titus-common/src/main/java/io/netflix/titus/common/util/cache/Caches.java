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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.netflix.spectator.api.Registry;
import io.netflix.titus.common.util.cache.internal.InstrumentedCache;

public class Caches {
    public static <K, V> Cache<K, V> instrumentedCacheWithMaxSize(long maxSize, String metricNameRoot, Registry registry) {
        com.github.benmanes.caffeine.cache.Cache<K, V> cache = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .recordStats()
                .build();
        return new InstrumentedCache<>(metricNameRoot, cache, registry);
    }
}
