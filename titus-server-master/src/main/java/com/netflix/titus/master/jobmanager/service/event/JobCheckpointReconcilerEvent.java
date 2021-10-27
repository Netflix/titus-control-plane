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

package com.netflix.titus.master.jobmanager.service.event;

import java.util.Collections;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.api.model.callmetadata.Caller;
import com.netflix.titus.api.model.callmetadata.CallerType;

public class JobCheckpointReconcilerEvent extends JobManagerReconcilerEvent {

    private static final Job<?> SENTINEL_JOB = Job.newBuilder().build();

    private static final CallMetadata CHECKPOINT_CALL_METADATA = CallMetadata.newBuilder()
            .withCallers(Collections.singletonList(
                    Caller.newBuilder()
                            .withId("reconciler")
                            .withCallerType(CallerType.Application)
                            .build()
            ))
            .withCallReason("Job event stream checkpoint")
            .build();

    private final long timestampNano;

    public JobCheckpointReconcilerEvent(long timestampNano) {
        super(SENTINEL_JOB, "no-trx:checkpoint", CHECKPOINT_CALL_METADATA);
        this.timestampNano = timestampNano;
    }

    public long getTimestampNano() {
        return timestampNano;
    }

    @Override
    public String toString() {
        return "JobCheckpointReconcilerEvent{" +
                "timestampNano=" + timestampNano +
                '}';
    }
}
