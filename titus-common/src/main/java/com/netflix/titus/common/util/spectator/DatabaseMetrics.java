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

    private static final long[] LEVELS = new long[]{1, 10, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 60_000};

    private static final String ROOT_NAME = ".jooq.";
    private static final String OPERATION_LATENCY = "latency";
    private static final String OPERATION_ERROR = "error";

    private static final String STATUS_TAG = "status";
    private static final String RECORD_OP_TAG = "operation";
    private static final String RECORD_COUNT_TAG = "count";
    private static final String RECORD_TABLE_TAG = "table";
    private static final String RECORD_ERROR_CLASS = "class";

    private final Id operationLatency;
    private final Id errorCounter;
    private final MetricSelector<ValueRangeCounter> latencyBuckets;

    private enum Operations {
        INSERT,
        SELECT,
        SCAN,
        DELETE,
    }

    private final Registry registry;

    public DatabaseMetrics(Registry registry, String metricsNamespace, String databaseName) {
        List<Tag> commonTags = Collections.emptyList();

        this.registry = registry;

        String metricsRoot = metricsNamespace + ROOT_NAME;
        this.operationLatency = registry.createId(metricsRoot + databaseName + "." + OPERATION_LATENCY, commonTags);
        this.errorCounter = registry.createId(metricsRoot + databaseName + "." + OPERATION_ERROR, commonTags);
        this.latencyBuckets = SpectatorExt.newValueRangeCounterSortable(
                registry.createId(metricsRoot + databaseName + ".latencyBuckets"),
                new String[]{RECORD_TABLE_TAG, RECORD_OP_TAG, STATUS_TAG},
                LEVELS,
                registry
        );
    }

    public void registerInsertLatency(long startTimeMs, int numRecordsInserted, String tableName, List<Tag> additionalTags) {
        List<Tag> tags = new ArrayList<>();
        tags.add(new BasicTag(RECORD_COUNT_TAG, Integer.toString(numRecordsInserted)));

        registerLatency(Operations.INSERT, tableName, Stream.concat(tags.stream(),
                        additionalTags.stream()).collect(Collectors.toList()),
                System.currentTimeMillis() - startTimeMs);
    }

    public void registerScanLatency(long startTimeMs, String tableName, List<Tag> additionalTags) {
        registerLatency(Operations.SCAN, tableName, additionalTags, System.currentTimeMillis() - startTimeMs);
    }

    public void registerSelectLatency(long startTimeMs, String tableName, List<Tag> additionalTags) {
        registerLatency(Operations.SELECT, tableName, additionalTags, System.currentTimeMillis() - startTimeMs);
    }

    public void registerDeleteLatency(long startTimeMs, int numRecordsDeleted, String tableName, List<Tag> additionalTags) {
        List<Tag> tags = new ArrayList<>();
        tags.add(new BasicTag(RECORD_COUNT_TAG, Integer.toString(numRecordsDeleted)));

        registerLatency(Operations.DELETE, tableName, Stream.concat(tags.stream(),
                        additionalTags.stream()).collect(Collectors.toList()),
                System.currentTimeMillis() - startTimeMs);
    }

    public void registerInsertError(long startTimeMs, String tableName, Throwable throwable, Iterable<Tag> additionalTags) {
        registerError(Operations.INSERT, tableName, throwable, additionalTags, System.currentTimeMillis() - startTimeMs);
    }

    public void registerInsertError(String tableName, Throwable throwable, Iterable<Tag> additionalTags) {
        registerError(Operations.INSERT, tableName, throwable, additionalTags, 0);
    }

    public void registerSelectError(long startTimeMs, String tableName, Throwable throwable, Iterable<Tag> additionalTags) {
        registerError(Operations.SELECT, tableName, throwable, additionalTags, System.currentTimeMillis() - startTimeMs);
    }

    public void registerSelectError(String tableName, Throwable throwable, Iterable<Tag> additionalTags) {
        registerError(Operations.SELECT, tableName, throwable, additionalTags, 0);
    }

    public void registerDeleteError(long startTimeMs, String tableName, Throwable throwable, Iterable<Tag> additionalTags) {
        registerError(Operations.DELETE, tableName, throwable, additionalTags, System.currentTimeMillis() - startTimeMs);
    }

    public void registerDeleteError(String tableName, Throwable throwable, Iterable<Tag> additionalTags) {
        registerError(Operations.DELETE, tableName, throwable, additionalTags, 0);
    }

    private void registerLatency(Operations op, String tableName, List<Tag> additionalTags, long latencyMs) {
        Id delayId = operationLatency
                .withTags(RECORD_TABLE_TAG, tableName)
                .withTag(RECORD_OP_TAG, op.name())
                .withTags(additionalTags);
        registry.timer(delayId).record(latencyMs, TimeUnit.MILLISECONDS);
        // Tag order: RECORD_TABLE_TAG, RECORD_OP_TAG, STATUS_TAG
        latencyBuckets.withTags(tableName, op.name(), "success").ifPresent(m -> m.recordLevel(latencyMs));
    }

    private void registerError(Operations op, String tableName, Throwable throwable, Iterable<Tag> additionalTags, long latencyMs) {
        registry.counter(errorCounter
                        .withTag(RECORD_OP_TAG, op.name())
                        .withTag(RECORD_TABLE_TAG, tableName)
                        .withTags(RECORD_ERROR_CLASS, throwable.getClass().getSimpleName())
                        .withTags(additionalTags))
                .increment();
        // Tag order: RECORD_TABLE_TAG, RECORD_OP_TAG, STATUS_TAG
        latencyBuckets.withTags(tableName, op.name(), "error").ifPresent(m -> m.recordLevel(latencyMs));
    }
}
