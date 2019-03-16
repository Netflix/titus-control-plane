/*
 * Copyright 2019 Netflix, Inc.
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;

/**
 * This class contains generic methods to publish database-specific metrics.
 */
public class DatabaseMetrics {

    private static final String ROOT_NAME = "titus.jooq.";
    private static final String OPERATION_LATENCY = "latency";

    private static final String RECORD_OP_TAG = "operation";
    private static final String RECORD_COUNT_TAG = "count";

    private final Id operationLatency;

    private enum Operations {
        INSERT,
        SELECT,
        SCAN,
    }

    private final Registry registry;

    public DatabaseMetrics(Registry registry, String databaseName) {
        List<Tag> commonTags = Collections.emptyList();

        this.registry = registry;

        this.operationLatency = registry.createId(ROOT_NAME + databaseName + "." + OPERATION_LATENCY, commonTags);
    }

    public void registerInsertLatency(long startTimeMs, int numRecordsInserted, List<Tag> additionalTags) {
        List<Tag> tags = new ArrayList<>();
        tags.add(new BasicTag(RECORD_COUNT_TAG, Integer.toString(numRecordsInserted)));

        registerLatency(Operations.INSERT, Stream.concat(tags.stream(),
                additionalTags.stream()).collect(Collectors.toList()),
                System.currentTimeMillis() - startTimeMs);
    }

    public void registerScanLatency(long startTimeMs, List<Tag> additionalTags) {
        registerLatency(Operations.SCAN, additionalTags, System.currentTimeMillis() - startTimeMs);
    }

    public void registerSelectLatency(long startTimeMs, List<Tag> additionalTags) {
        registerLatency(Operations.SELECT, additionalTags, System.currentTimeMillis() - startTimeMs);
    }

    private void registerLatency(Operations op, List<Tag> additionalTags, long latencyMs) {
        Id delayId = operationLatency
                .withTag(RECORD_OP_TAG, op.name())
                .withTags(additionalTags);
        registry.timer(delayId).record(latencyMs, TimeUnit.MILLISECONDS);
    }
}
