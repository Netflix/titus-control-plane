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

package com.netflix.titus.common.framework.scheduler.model;

import java.util.Objects;
import java.util.Optional;

import com.google.common.base.Preconditions;

public class SchedulingStatus {

    public enum SchedulingState {
        Waiting,
        Running,
        Cancelling,
        Succeeded,
        Failed;

        public boolean isFinal() {
            return this == Succeeded || this == Failed;
        }
    }

    private final SchedulingState state;
    private final long timestamp;
    private final long expectedStartTime;
    private final Optional<Throwable> error;

    private SchedulingStatus(SchedulingState state, long timestamp, long expectedStartTime, Optional<Throwable> error) {
        this.state = state;
        this.timestamp = timestamp;
        this.expectedStartTime = expectedStartTime;
        this.error = error;
    }

    public SchedulingState getState() {
        return state;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getExpectedStartTime() {
        return expectedStartTime;
    }

    public Optional<Throwable> getError() {
        return error;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SchedulingStatus that = (SchedulingStatus) o;
        return timestamp == that.timestamp &&
                expectedStartTime == that.expectedStartTime &&
                state == that.state &&
                Objects.equals(error, that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, timestamp, expectedStartTime, error);
    }

    public Builder toBuilder() {
        return newBuilder().withState(state).withTimestamp(timestamp).withExpectedStartTime(expectedStartTime).withError(error.orElse(null));
    }

    @Override
    public String toString() {
        return "SchedulingStatus{" +
                "state=" + state +
                ", timestamp=" + timestamp +
                ", expectedStartTime=" + expectedStartTime +
                ", error=" + error +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private SchedulingState state;
        private long timestamp;
        private long expectedStartTime;
        private Optional<Throwable> error;

        private Builder() {
        }

        public Builder withState(SchedulingState state) {
            this.state = state;
            return this;
        }

        public Builder withTimestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder withExpectedStartTime(long expectedStartTime) {
            this.expectedStartTime = expectedStartTime;
            return this;
        }

        public Builder withError(Throwable error) {
            this.error = Optional.ofNullable(error);
            return this;
        }

        public SchedulingStatus build() {
            Preconditions.checkNotNull(state, "state cannot be null");

            return new SchedulingStatus(state, timestamp, expectedStartTime, error);
        }
    }
}
