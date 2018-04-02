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

package com.netflix.titus.api.jobmanager.model.job.ext;

import javax.validation.Valid;
import javax.validation.constraints.Min;

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.retry.RetryPolicy;
import com.netflix.titus.common.model.sanitizer.FieldInvariant;
import com.netflix.titus.common.model.sanitizer.ClassFieldsNotNull;

/**
 */
@ClassFieldsNotNull
public class BatchJobExt implements JobDescriptor.JobDescriptorExt {

    @Min(value = 1, message = "Batch job must have at least one task")
    @FieldInvariant(value = "value <= @constraints.getMaxBatchJobSize()", message = "Batch job too big #{value} > #{@constraints.getMaxBatchJobSize()}")
    private final int size;

    @Min(value = 60_000, message = "Runtime limit too low (must be at least 60sec, but is #{#root}[ms])")
    @FieldInvariant(value = "value <= @constraints.getMaxRuntimeLimitSec() * 1000", message = "Runtime limit too high #{value} > #{@constraints.getMaxRuntimeLimitSec() * 1000}")
    private final long runtimeLimitMs;

    @Valid
    private final RetryPolicy retryPolicy;

    public BatchJobExt(int size, long runtimeLimitMs, RetryPolicy retryPolicy) {
        this.size = size;
        this.runtimeLimitMs = runtimeLimitMs;
        this.retryPolicy = retryPolicy;
    }

    public int getSize() {
        return size;
    }

    public long getRuntimeLimitMs() {
        return runtimeLimitMs;
    }

    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BatchJobExt that = (BatchJobExt) o;

        if (size != that.size) {
            return false;
        }
        if (runtimeLimitMs != that.runtimeLimitMs) {
            return false;
        }
        return retryPolicy != null ? retryPolicy.equals(that.retryPolicy) : that.retryPolicy == null;
    }

    @Override
    public int hashCode() {
        int result = size;
        result = 31 * result + (int) (runtimeLimitMs ^ (runtimeLimitMs >>> 32));
        result = 31 * result + (retryPolicy != null ? retryPolicy.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "BatchJobExt{" +
                "size=" + size +
                ", runtimeLimitMs=" + runtimeLimitMs +
                ", retryPolicy=" + retryPolicy +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder(this);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(BatchJobExt batchJobExt) {
        return new Builder()
                .withRuntimeLimitMs(batchJobExt.getRuntimeLimitMs())
                .withRetryPolicy(batchJobExt.getRetryPolicy())
                .withSize(batchJobExt.getSize());
    }

    public static final class Builder {
        private int size;
        private long runtimeLimitMs;
        private RetryPolicy retryPolicy;

        private Builder() {
        }

        public Builder withSize(int size) {
            this.size = size;
            return this;
        }

        public Builder withRuntimeLimitMs(long runtimeLimitMs) {
            this.runtimeLimitMs = runtimeLimitMs;
            return this;
        }

        public Builder withRetryPolicy(RetryPolicy retryPolicy) {
            this.retryPolicy = retryPolicy;
            return this;
        }

        public BatchJobExt build() {
            BatchJobExt batchJobExt = new BatchJobExt(size, runtimeLimitMs, retryPolicy);
            return batchJobExt;
        }
    }
}
