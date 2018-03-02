/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.api.jobmanager.model.job;

import io.netflix.titus.common.model.sanitizer.ClassFieldsNotNull;

/**
 */
@ClassFieldsNotNull
public class JobStatus extends ExecutableStatus<JobState> {
    public JobStatus(JobState state, String reasonCode, String reasonMessage, long timestamp) {
        super(state, reasonCode, reasonMessage, timestamp);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(JobStatus jobStatus) {
        return new Builder(jobStatus);
    }

    public static class Builder extends AbstractBuilder<JobState, Builder, JobStatus> {

        private Builder() {
        }

        private Builder(JobStatus jobStatus) {
            super(jobStatus);
        }

        @Override
        public JobStatus build() {
            return new JobStatus(state, reasonCode, toCompleteReasonMessage(), timestamp);
        }
    }
}
