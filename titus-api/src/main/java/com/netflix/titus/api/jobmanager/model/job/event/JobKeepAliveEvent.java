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

package com.netflix.titus.api.jobmanager.model.job.event;

import java.util.Optional;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.model.callmetadata.CallMetadata;

public class JobKeepAliveEvent extends JobManagerEvent<Job> {

    private static final CallMetadata KEEP_ALIVE_CALL_METADATA = CallMetadata.newBuilder()
            .withCallerId("KeepAliveEvent")
            .withCallReason("keep alive event")
            .build();

    private final long timestamp;

    JobKeepAliveEvent(long timestamp) {
        super(Job.newBuilder().build(), Optional.empty(), false, KEEP_ALIVE_CALL_METADATA);
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "JobKeepAliveEvent{" +
                "timestamp=" + timestamp +
                '}';
    }

    public static JobKeepAliveEvent keepAliveEvent(long timestamp) {
        return new JobKeepAliveEvent(timestamp);
    }
}
