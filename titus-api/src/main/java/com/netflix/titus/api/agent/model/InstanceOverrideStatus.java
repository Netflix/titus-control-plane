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

package com.netflix.titus.api.agent.model;

import javax.validation.constraints.Min;

import com.netflix.titus.common.model.sanitizer.ClassFieldsNotNull;

@ClassFieldsNotNull
public class InstanceOverrideStatus {

    private static final InstanceOverrideStatus OVERRIDE_NONE = new InstanceOverrideStatus(InstanceOverrideState.None, "none", 0);

    private final InstanceOverrideState state;

    private final String detail;

    @Min(value = 0, message = "Negative timestamp value")
    private final long timestamp;

    public InstanceOverrideStatus(InstanceOverrideState state, String detail, long timestamp) {
        this.state = state;
        this.detail = detail;
        this.timestamp = timestamp;
    }

    public InstanceOverrideState getState() {
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

        InstanceOverrideStatus that = (InstanceOverrideStatus) o;

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
        return "InstanceOverrideStatus{" +
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

    public static InstanceOverrideStatus none() {
        return OVERRIDE_NONE;
    }

    public static final class Builder {
        private InstanceOverrideState state;
        private String detail;
        private long timestamp;

        private Builder() {
        }

        public Builder withState(InstanceOverrideState state) {
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

        public InstanceOverrideStatus build() {
            InstanceOverrideStatus instanceOverrideStatus = new InstanceOverrideStatus(state, detail, timestamp);
            return instanceOverrideStatus;
        }
    }
}
