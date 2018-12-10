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

package com.netflix.titus.supplementary.relocation.model;

import java.util.Objects;

public class DeschedulingFailure {

    private static final DeschedulingFailure LEGACY_JOB_DESCHEDULING_FAILURE = new DeschedulingFailure("Legacy job, with no disruption budget configured");

    private final String reasonMessage;

    public DeschedulingFailure(String reasonMessage) {
        this.reasonMessage = reasonMessage;
    }

    public String getReasonMessage() {
        return reasonMessage;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DeschedulingFailure failure = (DeschedulingFailure) o;
        return Objects.equals(reasonMessage, failure.reasonMessage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(reasonMessage);
    }

    @Override
    public String toString() {
        return "DeschedulingFailure{" +
                "reasonMessage='" + reasonMessage + '\'' +
                '}';
    }

    public Builder but() {
        return newBuilder().withReasonMessage(reasonMessage);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static DeschedulingFailure legacyJobDeschedulingFailure() {
        return LEGACY_JOB_DESCHEDULING_FAILURE;
    }

    public static final class Builder {
        private String reasonMessage;

        private Builder() {
        }

        public Builder withReasonMessage(String reasonMessage) {
            this.reasonMessage = reasonMessage;
            return this;
        }

        public DeschedulingFailure build() {
            return new DeschedulingFailure(reasonMessage);
        }
    }
}
