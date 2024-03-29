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

package com.netflix.titus.api.jobmanager.model.job;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.validation.constraints.Min;

import com.netflix.titus.common.model.sanitizer.ClassFieldsNotNull;

import static com.netflix.titus.common.util.CollectionsExt.nonNull;

@ClassFieldsNotNull
public class BatchJobTask extends Task {

    @Min(value = 0)
    private final int index;

    public BatchJobTask(String id,
                        int index,
                        String originalId,
                        Optional<String> resubmitOf,
                        String jobId,
                        int resubmitNumber,
                        int systemResubmitNumber,
                        int evictionResubmitNumber,
                        TaskStatus status,
                        List<TaskStatus> statusHistory,
                        List<TwoLevelResource> twoLevelResources,
                        Map<String, String> taskContext,
                        Map<String, String> attributes,
                        Version version) {
        super(id, jobId, status, statusHistory, originalId, resubmitOf, resubmitNumber, systemResubmitNumber,
                evictionResubmitNumber, twoLevelResources, taskContext, attributes, version);
        this.index = index;
    }

    public int getIndex() {
        return index;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        BatchJobTask that = (BatchJobTask) o;

        return index == that.index;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + index;
        return result;
    }

    @Override
    public String toString() {
        return "BatchJobTask{" +
                "id='" + getId() + '\'' +
                ", index=" + index +
                ", jobId='" + getJobId() + '\'' +
                ", status=" + getStatus() +
                ", originalId='" + getOriginalId() + '\'' +
                ", resubmitOf=" + getResubmitOf() +
                ", resubmitNumber=" + getResubmitNumber() +
                ", systemResubmitNumber=" + getSystemResubmitNumber() +
                ", evictionResubmitNumber=" + getEvictionResubmitNumber() +
                ", twoLevelResources=" + getTwoLevelResources() +
                ", taskContext=" + getTaskContext() +
                ", attributes=" + getAttributes() +
                ", version=" + getVersion() +
                '}';
    }

    @Override
    public Builder toBuilder() {
        return newBuilder(this);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(BatchJobTask task) {
        return new Builder(task);
    }

    public static final class Builder extends TaskBuilder<BatchJobTask, Builder> {

        private int index;

        private Builder() {
        }

        private Builder(BatchJobTask batchJobTask) {
            newBuilder(this, batchJobTask).withIndex(batchJobTask.getIndex());
        }

        public Builder but() {
            return but(new Builder()).withIndex(index);
        }

        public Builder withIndex(int index) {
            this.index = index;
            return this;
        }

        @Override
        public BatchJobTask build() {
            return new BatchJobTask(
                    id,
                    index,
                    originalId == null ? id : originalId,
                    Optional.ofNullable(resubmitOf),
                    jobId,
                    resubmitNumber,
                    systemResubmitNumber,
                    evictionResubmitNumber,
                    status,
                    nonNull(statusHistory),
                    nonNull(twoLevelResources),
                    nonNull(taskContext),
                    nonNull(attributes),
                    version
            );
        }
    }
}
