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

package com.netflix.titus.common.jhiccup.sensor;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.netflix.spectator.api.Registry;
import org.HdrHistogram.Histogram;

class HiccupSensorMetrics {

    private static final double[] RECORDED_PERCENTILES = {90.0, 95.0, 99.0, 99.5, 99.95, 99.995};

    private final String name;
    private final Map<String, String> tags;
    private final Registry registry;
    private final ConcurrentMap<Double, AtomicLong> hiccupMetrics = new ConcurrentHashMap<>();

    HiccupSensorMetrics(String name, Registry registry) {
        this(name, Collections.emptyMap(), registry);
    }

    HiccupSensorMetrics(String name, Map<String, String> tags, Registry registry) {
        this.name = name;
        this.tags = tags;
        this.registry = registry;
    }

    void updateMetrics(Histogram intervalHistogram) {
        for (double percentile : RECORDED_PERCENTILES) {
            AtomicLong metric = hiccupMetrics.get(percentile);
            if (metric == null) {
                metric = new AtomicLong();
                Map<String, String> myTags = new HashMap<>(tags);
                myTags.put("percentile", Double.toString(percentile));
                registry.gauge(registry.createId(name + ".histogram", myTags), metric);
                hiccupMetrics.put(percentile, metric);
            }
            long value = intervalHistogram.getValueAtPercentile(percentile);
            metric.set(TimeUnit.NANOSECONDS.toMillis(value));
        }
    }
}
