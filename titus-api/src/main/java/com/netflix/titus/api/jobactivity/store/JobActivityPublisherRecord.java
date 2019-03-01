/*
 *
 *  * Copyright 2019 Netflix, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.netflix.titus.api.jobactivity.store;

import java.beans.ConstructorProperties;

/**
 * Represents a published job activity event record. The event record may represent
 * activity on a job or task object.
 */
public class JobActivityPublisherRecord {
    /**
     * Represents type of event record described.
     */
    public enum RecordType {
        JOB,
        TASK
    }

    // TODO(Andrew L): Add CallMetadata when ready
    private final long queueIndex;
    private final RecordType recordType;
    private final byte[] serializedEvent;

    @ConstructorProperties({ "queue_index", "event_type", "serialized_event" })
    public JobActivityPublisherRecord(long queueIndex, short recordShort, byte[] serializedEvent) {
        this.queueIndex = queueIndex;
        this.recordType = RecordType.values()[recordShort];
        this.serializedEvent = serializedEvent;
    }

    public long getQueueIndex() {
        return queueIndex;
    }

    public RecordType getRecordType() {
        return recordType;
    }

    public byte[] getSerializedEvent() {
        return serializedEvent;
    }
}
