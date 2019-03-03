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

package com.netflix.titus.api.jobactivity.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import com.netflix.titus.api.jobactivity.store.JobActivityPublisherRecord;

public class JobActivityPublisherMetrics {

    private static final String ROOT_NAME = "titus.jobactivity.publish.";
    private static final String PUBLISH_SUCCESS = ROOT_NAME + "success";
    private static final String PUBLISH_ERRORS = ROOT_NAME + "errors";
    private static final String PUBLISH_LATENCY = ROOT_NAME + "latency";

    private static final String RECORD_TYPE_TAG = "type";
    private static final String RECORD_ID_TAG = "id";
    private static final String RECORD_REASON_TAG = "reason";
    private static final String RECORD_OP_TAG = "operation";
    private static final String RECORD_COUNT_TAG = "count";

    private enum Operations {
        INSERT,
        SELECT,
        SCAN,
    }

    private final Registry registry;

    private final Id publishSuccess;
    private final Id publishErrors;
    private final Id publishLatency;

    public JobActivityPublisherMetrics(Registry registry) {
        this.registry = registry;

        List<Tag> commonTags = Collections.emptyList();
        this.publishSuccess = registry.createId(PUBLISH_SUCCESS, commonTags);
        this.publishErrors = registry.createId(PUBLISH_ERRORS, commonTags);
        this.publishLatency = registry.createId(PUBLISH_LATENCY, commonTags);
    }

    public void publishSuccess(JobActivityPublisherRecord.RecordType recordType) {
        registry.counter(publishSuccess
                .withTag(RECORD_TYPE_TAG, recordType.name())
        ).increment();
    }

    public void publishError(JobActivityPublisherRecord.RecordType recordType, String recordId, Throwable throwable) {
        registry.counter(publishErrors
                .withTag(RECORD_TYPE_TAG, recordType.name())
                .withTag(RECORD_ID_TAG, recordId)
                .withTag(RECORD_REASON_TAG, throwable.getMessage())
        ).increment();
    }

    public void registerInsertLatency(JobActivityPublisherRecord.RecordType recordType, int numRecords, long startTime) {
        List<Tag> tags = new ArrayList<>();
        tags.add(new BasicTag(RECORD_TYPE_TAG, recordType.name()));
        tags.add(new BasicTag(RECORD_COUNT_TAG, Integer.toString(numRecords)));
        registerLatency(Operations.INSERT, tags,System.currentTimeMillis() - startTime);
    }

    public void registerSelectLatency(long startTime) {
        registerLatency(Operations.SELECT, Collections.emptyList(), System.currentTimeMillis() - startTime);
    }

    public void registerScanLatency(long startTime) {
        registerLatency(Operations.SCAN, Collections.emptyList(), System.currentTimeMillis() - startTime);
    }

    private void registerLatency(Operations op, List<Tag> additionalTags, long latency) {
        Id delayId = publishLatency
                .withTag(RECORD_OP_TAG, op.name())
                .withTags(additionalTags);
        registry.timer(delayId).record(latency, TimeUnit.MILLISECONDS);
    }
}
