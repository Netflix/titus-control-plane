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

package com.netflix.titus.master.jobmanager.service.event;

import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.service.JobManagerConstants;

/**
 */
public abstract class JobManagerReconcilerEvent {

    private final Job<?> job;
    private final String transactionId;
    private final CallMetadata callMetadata;

    JobManagerReconcilerEvent(Job<?> job, String transactionId, CallMetadata callMetadata) {
        this.job = job;
        this.transactionId = transactionId;
        this.callMetadata = callMetadata;
    }

    public Job<?> getJob() {
        return job;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public CallMetadata getCallMetadata() {
        if(callMetadata == null) {
            return JobManagerConstants.UNDEFINED_CALL_METADATA;
        }
        return callMetadata;
    }
}
