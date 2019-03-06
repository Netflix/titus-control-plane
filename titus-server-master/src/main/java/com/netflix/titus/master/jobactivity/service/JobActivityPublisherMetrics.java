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

package com.netflix.titus.master.jobactivity.service;

import java.util.Collections;
import java.util.List;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import com.netflix.titus.api.jobactivity.store.JobActivityPublisherRecord;

public class JobActivityPublisherMetrics {

    private static final String ROOT_NAME = "titus.jobactivity.publish.";
    private static final String PUBLISH_SUCCESS = ROOT_NAME + "success";
    private static final String PUBLISH_ERRORS = ROOT_NAME + "errors";

    private static final String RECORD_TYPE_TAG = "type";
    private static final String RECORD_ID_TAG = "id";
    private static final String RECORD_REASON_TAG = "reason";

    private final Registry registry;

    private final Id publishSuccess;
    private final Id publishErrors;

    public JobActivityPublisherMetrics(Registry registry) {
        this.registry = registry;

        List<Tag> commonTags = Collections.emptyList();
        this.publishSuccess = registry.createId(PUBLISH_SUCCESS, commonTags);
        this.publishErrors = registry.createId(PUBLISH_ERRORS, commonTags);
    }

    public void publishSuccess(JobActivityPublisherRecord.RecordType recordType) {
        registry.counter(publishSuccess
                .withTag(RECORD_TYPE_TAG, recordType.name())
        ).increment();
    }

    public void publishDropError() {
        registry.counter(publishErrors
                .withTag(RECORD_REASON_TAG, "backPressureDrop")
        ).increment();
    }

    public void publishError(JobActivityPublisherRecord.RecordType recordType, String recordId, Throwable throwable) {
        publishError(recordType, recordId, throwable.getMessage());
    }

    public void publishError(JobActivityPublisherRecord.RecordType recordType, String recordId, String reason) {
        registry.counter(publishErrors
                .withTag(RECORD_TYPE_TAG, recordType.name())
                .withTag(RECORD_ID_TAG, recordId)
                .withTag(RECORD_REASON_TAG, reason)
        ).increment();
    }
}
