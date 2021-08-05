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

package com.netflix.titus.common.util.spectator;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import com.netflix.spectator.api.Id;

class DynamicTagProcessor<METRIC> implements MetricSelector<METRIC> {

    private final Id rootId;
    private final String[] tagNames;
    private final Function<Id, METRIC> metricFactory;
    private final ConcurrentMap<Tags, METRIC> metrics = new ConcurrentHashMap<>();

    DynamicTagProcessor(Id rootId,
                        String[] tagNames,
                        Function<Id, METRIC> metricFactory) {
        this.rootId = rootId;
        this.tagNames = tagNames;
        this.metricFactory = metricFactory;
    }

    @Override
    public Optional<METRIC> withTags(String... tagValues) {
        if (tagValues.length != tagNames.length) {
            return Optional.empty();
        }
        Tags tags = new Tags(tagNames, tagValues);
        METRIC metric = metrics.get(tags);
        if (metric != null) {
            return Optional.of(metric);
        }
        metric = metrics.computeIfAbsent(tags, t -> {
            Id id = buildIdOf(tagValues);
            return metricFactory.apply(id);
        });
        return Optional.of(metric);
    }

    private Id buildIdOf(String[] tagValues) {
        Id id = rootId;
        for (int i = 0; i < tagValues.length; i++) {
            id = id.withTag(tagNames[i], tagValues[i]);
        }
        return id;
    }

    private static class Tags {

        private final Map<String, String> tagMap;

        private Tags(String[] tagNames, String[] tagValues) {
            Map<String, String> tagMap = new HashMap<>();
            for (int i = 0; i < tagNames.length; i++) {
                tagMap.put(tagNames[i], tagValues[i]);
            }
            this.tagMap = tagMap;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Tags tags = (Tags) o;
            return Objects.equals(tagMap, tags.tagMap);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tagMap);
        }
    }
}
