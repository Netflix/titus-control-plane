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

import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.netflix.titus.common.model.sanitizer.NeverNull;

import static io.netflix.titus.common.util.CollectionsExt.nonNull;

/**
 */
@NeverNull
public class ServiceJobTask extends Task {

    private ServiceJobTask(String id,
                           String originalId,
                           Optional<String> resubmitOf,
                           String jobId,
                           int resubmitNumber,
                           TaskStatus status,
                           List<TaskStatus> statusHistory,
                           List<TwoLevelResource> twoLevelResources,
                           Map<String, String> taskContext) {
        super(id, jobId, status, statusHistory, originalId, resubmitOf, resubmitNumber, twoLevelResources, taskContext);
    }

    @Override
    public Builder toBuilder() {
        return newBuilder(this);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(ServiceJobTask serviceJobTask) {
        return new Builder(serviceJobTask);
    }

    public static class Builder extends TaskBuilder<ServiceJobTask, Builder> {

        private Builder() {
        }

        private Builder(ServiceJobTask serviceJobTask) {
            newBuilder(this, serviceJobTask);
        }

        public Builder but() {
            return but(new Builder());
        }

        @Override
        public ServiceJobTask build() {
            return new ServiceJobTask(id,
                    originalId,
                    Optional.ofNullable(resubmitOf),
                    jobId,
                    resubmitNumber,
                    status,
                    nonNull(statusHistory),
                    nonNull(twoLevelResources),
                    nonNull(taskContext)
            );
        }
    }
}
