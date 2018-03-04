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
 *
 */
@ClassFieldsNotNull
public class JobGroupInfo {

    private final String stack;
    private final String detail;
    private final String sequence;

    public JobGroupInfo(String stack, String detail, String sequence) {
        this.stack = stack;
        this.detail = detail;
        this.sequence = sequence;
    }

    public String getStack() {
        return stack;
    }

    public String getDetail() {
        return detail;
    }

    public String getSequence() {
        return sequence;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JobGroupInfo that = (JobGroupInfo) o;

        if (stack != null ? !stack.equals(that.stack) : that.stack != null) {
            return false;
        }
        if (detail != null ? !detail.equals(that.detail) : that.detail != null) {
            return false;
        }
        return sequence != null ? sequence.equals(that.sequence) : that.sequence == null;
    }

    @Override
    public int hashCode() {
        int result = stack != null ? stack.hashCode() : 0;
        result = 31 * result + (detail != null ? detail.hashCode() : 0);
        result = 31 * result + (sequence != null ? sequence.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "JobGroupInfo{" +
                "stack='" + stack + '\'' +
                ", detail='" + detail + '\'' +
                ", sequence='" + sequence + '\'' +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(JobGroupInfo jobGroupInfo) {
        return new Builder()
                .withStack(jobGroupInfo.getStack())
                .withDetail(jobGroupInfo.getDetail())
                .withSequence(jobGroupInfo.getSequence());
    }

    public static final class Builder {
        private String stack;
        private String detail;
        private String sequence;

        private Builder() {
        }

        public Builder withStack(String stack) {
            this.stack = stack;
            return this;
        }

        public Builder withDetail(String detail) {
            this.detail = detail;
            return this;
        }

        public Builder withSequence(String sequence) {
            this.sequence = sequence;
            return this;
        }

        public JobGroupInfo build() {
            JobGroupInfo jobGroupInfo = new JobGroupInfo(stack, detail, sequence);
            return jobGroupInfo;
        }
    }
}
