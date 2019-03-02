/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.api.supervisor.model;

import java.util.Objects;

public class MasterStatus {

    public static final String REASON_CODE_NORMAL = "normal";
    public static final String REASON_CODE_OUT_OF_SERVICE = "outOfService";
    public static final String REASON_CODE_UNHEALTHY = "unhealthy";

    private final MasterState state;
    private final String reasonCode;
    private final String reasonMessage;
    private final long timestamp;

    public MasterStatus(MasterState state, String reasonCode, String reasonMessage, long timestamp) {
        this.state = state;
        this.reasonCode = reasonCode;
        this.reasonMessage = reasonMessage;
        this.timestamp = timestamp;
    }

    public MasterState getState() {
        return state;
    }

    public String getReasonCode() {
        return reasonCode;
    }

    public String getReasonMessage() {
        return reasonMessage;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MasterStatus that = (MasterStatus) o;
        return timestamp == that.timestamp &&
                state == that.state &&
                Objects.equals(reasonCode, that.reasonCode) &&
                Objects.equals(reasonMessage, that.reasonMessage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, reasonCode, reasonMessage, timestamp);
    }

    @Override
    public String toString() {
        return "MasterStatus{" +
                "state=" + state +
                ", reasonCode='" + reasonCode + '\'' +
                ", reasonMessage='" + reasonMessage + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder().withState(state).withReasonCode(reasonCode).withReasonMessage(reasonMessage).withTimestamp(timestamp);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private MasterState state;
        private String reasonCode;
        private String reasonMessage;
        private long timestamp;

        private Builder() {
        }

        public Builder withState(MasterState state) {
            this.state = state;
            return this;
        }

        public Builder withReasonCode(String reasonCode) {
            this.reasonCode = reasonCode;
            return this;
        }

        public Builder withReasonMessage(String reasonMessage) {
            this.reasonMessage = reasonMessage;
            return this;
        }

        public Builder withTimestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder but() {
            return newBuilder().withState(state).withReasonCode(reasonCode).withReasonMessage(reasonMessage).withTimestamp(timestamp);
        }

        public MasterStatus build() {
            return new MasterStatus(state, reasonCode, reasonMessage, timestamp);
        }
    }
}
