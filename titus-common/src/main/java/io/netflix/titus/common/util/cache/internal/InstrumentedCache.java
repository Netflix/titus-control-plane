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

package io.netflix.titus.common.util.cache.internal;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.base.Preconditions;
import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Registry;
import io.netflix.titus.common.util.rx.ObservableExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Subscription;
import rx.schedulers.Schedulers;

public class InstrumentedCache<K, V> implements io.netflix.titus.common.util.cache.Cache<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(InstrumentedCache.class);
    private static final String UPDATE_CACHE_METRICS_NAME = "updateCacheMetrics";
    private static final long UPDATE_METRICS_INTERVAL_SEC = 60;

    private final Cache<K, V> cache;

    private final Subscription metricSubscription;
    private final Gauge requestCountGauge;
    private final Gauge hitCountGauge;
    private final Gauge missCountGauge;
    private final Gauge loadSuccessCountGauge;
    private final Gauge loadFailureCountGauge;
    private final Gauge totalLoadTimeGauge;
    private final Gauge evictionCountGauge;
    private final Gauge evictionWeightGauge;
    private final Gauge estimatedSizeGauge;

    private volatile CacheStats lastStatsSnapshot;

    public InstrumentedCache(String metricNameRoot, Cache<K, V> cache, Registry registry) {
        this.cache = cache;

        Preconditions.checkNotNull(registry, "registry");
        Preconditions.checkNotNull(cache, "cache");

        String metricPrefix = metricNameRoot + ".cache.";
        requestCountGauge = registry.gauge(metricPrefix + "requestCount");
        hitCountGauge = registry.gauge(metricPrefix + "hitCount");
        missCountGauge = registry.gauge(metricPrefix + "missCount");
        loadSuccessCountGauge = registry.gauge(metricPrefix + "loadSuccessCount");
        loadFailureCountGauge = registry.gauge(metricPrefix + "loadFailureCount");
        totalLoadTimeGauge = registry.gauge(metricPrefix + "totalLoadTime");
        evictionCountGauge = registry.gauge(metricPrefix + "evictionCount");
        evictionWeightGauge = registry.gauge(metricPrefix + "evictionWeight");
        estimatedSizeGauge = registry.gauge(metricPrefix + "estimatedSize");

        metricSubscription = ObservableExt.schedule(metricNameRoot, registry, UPDATE_CACHE_METRICS_NAME,
                Completable.fromAction(this::updateMetrics), 0, UPDATE_METRICS_INTERVAL_SEC, TimeUnit.SECONDS, Schedulers.computation()
        ).subscribe(
                next -> {
                    if (next.isPresent()) {
                        Throwable cause = next.get();
                        logger.error("Unable to update cache metrics with error: {}", cause);
                    } else {
                        logger.debug("Successfully updated cache metrics");
                    }
                }
        );
    }

    @CheckForNull
    @Override
    public V getIfPresent(@Nonnull Object key) {
        return cache.getIfPresent(key);
    }

    @CheckForNull
    @Override
    public V get(@Nonnull K key, @Nonnull Function<? super K, ? extends V> mappingFunction) {
        return cache.get(key, mappingFunction);
    }

    @Nonnull
    @Override
    public Map<K, V> getAllPresent(@Nonnull Iterable<?> keys) {
        return cache.getAllPresent(keys);
    }

    @Override
    public void put(@Nonnull K key, @Nonnull V value) {
        cache.put(key, value);
    }

    @Override
    public void putAll(@Nonnull Map<? extends K, ? extends V> map) {
        cache.putAll(map);
    }

    @Override
    public void invalidate(@Nonnull Object key) {
        cache.invalidate(key);
    }

    @Override
    public void invalidateAll(@Nonnull Iterable<?> keys) {
        cache.invalidateAll(keys);
    }

    @Override
    public void invalidateAll() {
        cache.invalidateAll();
    }

    @Override
    public long estimatedSize() {
        return cache.estimatedSize();
    }

    @Nonnull
    @Override
    public ConcurrentMap<K, V> asMap() {
        return cache.asMap();
    }

    @Override
    public void cleanUp() {
        cache.cleanUp();
    }

    @Override
    public void shutdown() {
        ObservableExt.safeUnsubscribe(metricSubscription);
        invalidateAll();
        cleanUp();
    }

    private void updateMetrics() {
        CacheStats currentStatsSnapshot = cache.stats();
        if (lastStatsSnapshot != null) {
            CacheStats statsDifference = currentStatsSnapshot.minus(lastStatsSnapshot);
            requestCountGauge.set(statsDifference.requestCount());
            hitCountGauge.set(statsDifference.hitCount());
            missCountGauge.set(statsDifference.missCount());
            loadSuccessCountGauge.set(statsDifference.loadSuccessCount());
            loadFailureCountGauge.set(statsDifference.loadFailureCount());
            totalLoadTimeGauge.set(statsDifference.totalLoadTime());
            evictionCountGauge.set(statsDifference.evictionCount());
            evictionWeightGauge.set(statsDifference.evictionWeight());
        }
        estimatedSizeGauge.set(estimatedSize());
        lastStatsSnapshot = currentStatsSnapshot;
    }
}
