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
public class InstanceLifecycleStatus {

    private final InstanceLifecycleState state;

    @Min(value = 0, message = "Negative launchTimestamp value")
    private final long launchTimestamp;

    public InstanceLifecycleStatus(InstanceLifecycleState state, long launchTimestamp) {
        this.state = state;
        this.launchTimestamp = launchTimestamp;
    }

    public InstanceLifecycleState getState() {
        return state;
    }

    public long getLaunchTimestamp() {
        return launchTimestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InstanceLifecycleStatus that = (InstanceLifecycleStatus) o;

        if (launchTimestamp != that.launchTimestamp) {
            return false;
        }
        return state == that.state;
    }

    @Override
    public int hashCode() {
        int result = state != null ? state.hashCode() : 0;
        result = 31 * result + (int) (launchTimestamp ^ (launchTimestamp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "InstanceLifecycleStatus{" +
                "state=" + state +
                ", launchTimestamp=" + launchTimestamp +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder().withState(state).withLaunchTimestamp(launchTimestamp);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private InstanceLifecycleState state;
        private long launchTimestamp;

        private Builder() {
        }

        public Builder withState(InstanceLifecycleState state) {
            this.state = state;
            return this;
        }

        public Builder withLaunchTimestamp(long launchTimestamp) {
            this.launchTimestamp = launchTimestamp;
            return this;
        }

        public Builder but() {
            return newBuilder().withState(state).withLaunchTimestamp(launchTimestamp);
        }

        public InstanceLifecycleStatus build() {
            return new InstanceLifecycleStatus(state, launchTimestamp);
        }
    }
}
