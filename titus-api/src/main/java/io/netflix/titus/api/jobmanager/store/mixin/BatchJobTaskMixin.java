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

package io.netflix.titus.api.jobmanager.store.mixin;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.api.jobmanager.model.job.TwoLevelResource;

public abstract class BatchJobTaskMixin {
    @JsonCreator
    BatchJobTaskMixin(@JsonProperty("id") String id,
                      @JsonProperty("index") int index,
                      @JsonProperty("originalId") String originalId,
                      @JsonProperty("resubmitOf") Optional<String> resubmitOf,
                      @JsonProperty("jobId") String jobId,
                      @JsonProperty("resubmitNumber") int resubmitNumber,
                      @JsonProperty("status") TaskStatus status,
                      @JsonProperty("statusHistory") List<TaskStatus> statusHistory,
                      @JsonProperty("twoLevelResources") List<TwoLevelResource> twoLevelResources,
                      @JsonProperty("taskContext") Map<String, String> taskContext) {
    }
}
