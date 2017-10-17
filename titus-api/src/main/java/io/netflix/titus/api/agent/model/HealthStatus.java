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

package io.netflix.titus.api.agent.model;

import javax.validation.constraints.Min;

import io.netflix.titus.common.model.sanitizer.NeverNull;

@NeverNull
public class HealthStatus {

    private static final HealthStatus UNKNOWN = new HealthStatus(HealthState.Unknown, "Health status not determined yet", 0);

    private final HealthState state;

    private final String detail;

    @Min(value = 0, message = "Negative timestamp value")
    private final long timestamp;

    public HealthStatus(HealthState state,
                        String detail,
                        long timestamp) {
        this.state = state;
        this.detail = detail;
        this.timestamp = timestamp;
    }

    public HealthState getState() {
        return state;
    }

    public String getDetail() {
        return detail;
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

        HealthStatus that = (HealthStatus) o;

        if (timestamp != that.timestamp) {
            return false;
        }
        if (state != that.state) {
            return false;
        }
        return detail != null ? detail.equals(that.detail) : that.detail == null;
    }

    @Override
    public int hashCode() {
        int result = state != null ? state.hashCode() : 0;
        result = 31 * result + (detail != null ? detail.hashCode() : 0);
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "HealthStatus{" +
                "state=" + state +
                ", detail='" + detail + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder().withState(state).withDetail(detail).withTimestamp(timestamp);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static HealthStatus unknwon() {
        return UNKNOWN;
    }

    public static final class Builder {
        private HealthState state;
        private String detail;
        private long timestamp;

        private Builder() {
        }

        public Builder withState(HealthState state) {
            this.state = state;
            return this;
        }

        public Builder withDetail(String detail) {
            this.detail = detail;
            return this;
        }

        public Builder withTimestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder but() {
            return newBuilder().withState(state).withDetail(detail).withTimestamp(timestamp);
        }

        public HealthStatus build() {
            HealthStatus healthStatus = new HealthStatus(state, detail, timestamp);
            return healthStatus;
        }
    }
}
