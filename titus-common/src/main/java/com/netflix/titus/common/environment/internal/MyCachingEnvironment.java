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

package com.netflix.titus.common.environment.internal;

import java.io.Closeable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import com.netflix.titus.common.environment.MyEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper around {@link MyEnvironment} that provides property values from cache. For all property that are requested,
 * refreshes them in the background with the configured interval. This wrapper should be used when the underlying
 * implementation is slow.
 */
public class MyCachingEnvironment implements MyEnvironment, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(MyCachingEnvironment.class);

    private static final String METRICS_ROOT = "titus.myEnvironment.cache.";

    private final String sourceId;
    private final MyEnvironment source;
    private final Thread updaterThread;

    private final ConcurrentMap<String, ValueHolder> cache = new ConcurrentHashMap<>();

    private final Counter updateCountMetric;
    private final Timer updateTimeMetric;

    public MyCachingEnvironment(String sourceId, MyEnvironment source, long refreshCycleMs, Registry registry) {
        this.sourceId = sourceId;
        this.source = source;
        this.updateCountMetric = registry.counter(METRICS_ROOT + "updateCount", "sourceId", sourceId);
        this.updateTimeMetric = registry.timer(METRICS_ROOT + "updateTime", "sourceId", sourceId);
        this.updaterThread = new Thread("environment-updater") {
            @Override
            public void run() {
                try {
                    while (true) {
                        refresh();
                        try {
                            Thread.sleep(refreshCycleMs);
                        } catch (InterruptedException e) {
                            break;
                        }
                    }
                } catch (Exception e) {
                    logger.error("[{}] Environment refresh process terminated with an error", sourceId, e);
                } finally {
                    logger.info("[{}] Environment refresh process terminated", sourceId);
                }
            }
        };
        updaterThread.start();
    }

    private void refresh() {
        int updatesCount = 0;
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            Set<String> keys = new HashSet<>(cache.keySet());
            for (String key : keys) {
                ValueHolder previous = cache.get(key);
                ValueHolder current = new ValueHolder(source.getProperty(key));
                if (previous != null && !Objects.equals(previous.getValue(), current.getValue())) {
                    updatesCount++;
                    cache.put(key, current);
                    logger.info("[{}] Property update: key={}, previous={}, current={}", sourceId, key, previous.getValue(), current.getValue());
                }
            }
        } catch (Exception e) {
            logger.error("[{}] Failed to refresh configuration properties", sourceId, e);
        }
        updateCountMetric.increment(updatesCount);
        updateTimeMetric.record(stopwatch.elapsed(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        updaterThread.interrupt();
    }

    @Override
    public String getProperty(String key) {
        ValueHolder valueHolder = cache.computeIfAbsent(key, k -> {
            ValueHolder current = new ValueHolder(source.getProperty(key));
            logger.info("[{}] Property first access: key={}, current={}", sourceId, key, current.getValue());
            return current;
        });
        return valueHolder.getValue();
    }

    @Override
    public String getProperty(String key, String defaultValue) {
        String value = getProperty(key);
        return value == null ? defaultValue : value;
    }

    private static class ValueHolder {

        private final String value;

        private ValueHolder(String value) {
            this.value = value;
        }

        private String getValue() {
            return value;
        }
    }
}
