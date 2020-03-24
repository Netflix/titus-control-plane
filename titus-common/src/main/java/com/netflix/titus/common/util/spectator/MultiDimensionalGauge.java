/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.common.util.spectator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import com.netflix.titus.common.util.CollectionsExt;

/**
 * See {@link SpectatorExt#multiDimensionalGauge(Id, Collection, Registry)}.
 */
public class MultiDimensionalGauge {

    private final Id rootId;
    private final int dimension;
    private final List<String> discerningTagNames;
    private final Registry registry;

    /**
     * Discerning tag names ordered from 0 to N-1. The number assigned to a tag name is used as an index in the
     * data structures in this class to address this tag's value. The order is consistent with the order in
     * {@link #discerningTagNames} list.
     */
    private final Map<String, Integer> tagNamesOrder;

    /**
     * Maps ordered list of values identifying each gauge to the gauge itself.
     */
    @VisibleForTesting
    final ConcurrentMap<List<String>, Gauge> gaugesByTagValues = new ConcurrentHashMap<>();

    private final AtomicLong nextRevision = new AtomicLong();
    private volatile long lastCommitted = -1;

    MultiDimensionalGauge(Id rootId, Collection<String> discerningTagNames, Registry registry) {
        Preconditions.checkArgument(discerningTagNames.size() > 0, "At least one discerning tag name expected");
        this.rootId = rootId;
        this.dimension = discerningTagNames.size();
        this.discerningTagNames = new ArrayList<>(discerningTagNames);
        this.tagNamesOrder = order(this.discerningTagNames);

        this.registry = registry;
    }

    public Setter beginUpdate() {
        return new Setter(nextRevision.getAndIncrement());
    }

    public void remove() {
        synchronized (gaugesByTagValues) {
            gaugesByTagValues.forEach((tagValues, gauge) -> gauge.set(0));
            gaugesByTagValues.clear();
        }
    }

    private Map<String, Integer> order(List<String> discerningTagNames) {
        Map<String, Integer> tagNamesOrder = new HashMap<>();
        for (int i = 0; i < discerningTagNames.size(); i++) {
            tagNamesOrder.put(discerningTagNames.get(i), i);
        }
        return tagNamesOrder;
    }

    public class Setter {

        private final long revisionId;
        private final Map<List<String>, Double> newValues = new HashMap<>();

        private Setter(long revisionId) {
            this.revisionId = revisionId;
        }

        public Setter set(List<String> keyValueList, double value) {
            Preconditions.checkArgument(keyValueList.size() == dimension * 2, "Incorrect number of key/value pairs");

            List<String> tagValues = new ArrayList<>(dimension);
            for (int i = 0; i < dimension; i += 1) {
                tagValues.add("");
            }
            for (int i = 0; i < dimension; i++) {
                String tagName = keyValueList.get(i * 2);
                Preconditions.checkArgument(tagNamesOrder.containsKey(tagName), "Invalid tag name: %s", tagName);
                int idx = tagNamesOrder.get(tagName);
                tagValues.set(idx, keyValueList.get(i * 2 + 1));
            }
            newValues.put(tagValues, value);

            return this;
        }

        public MultiDimensionalGauge commit() {
            synchronized (gaugesByTagValues) {
                // Silently ignore updates that older than the currently committed state.
                if (lastCommitted > revisionId) {
                    return MultiDimensionalGauge.this;
                }
                refreshGauges();
                lastCommitted = revisionId;
            }
            return MultiDimensionalGauge.this;
        }

        private void refreshGauges() {
            // Reset no longer observed gauges.
            Set<List<String>> toRemove = CollectionsExt.copyAndRemove(gaugesByTagValues.keySet(), newValues.keySet());
            toRemove.forEach(tagValues -> gaugesByTagValues.remove(tagValues).set(0));

            // Add/update observed gauges.
            newValues.forEach((tagValues, gaugeValue) -> gaugesByTagValues.computeIfAbsent(tagValues, this::newGauge).set(gaugeValue));
        }

        private Gauge newGauge(List<String> tagValues) {
            List<Tag> tags = new ArrayList<>(dimension);
            for (int i = 0; i < dimension; i++) {
                tags.add(new BasicTag(discerningTagNames.get(i), tagValues.get(i)));
            }
            return registry.gauge(rootId.withTags(tags));
        }
    }
}
