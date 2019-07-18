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

package com.netflix.titus.api.health;

import java.util.Map;
import java.util.Objects;

public class HealthStatus {

    private final HealthState healthState;
    private final Map<String, Object> details;

    public HealthStatus(HealthState healthState, Map<String, Object> details) {
        this.healthState = healthState;
        this.details = details;
    }

    public HealthState getHealthState() {
        return healthState;
    }

    public Map<String, Object> getDetails() {
        return details;
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
        return healthState == that.healthState &&
                Objects.equals(details, that.details);
    }

    @Override
    public int hashCode() {
        return Objects.hash(healthState, details);
    }

    @Override
    public String toString() {
        return "HealthStatus{" +
                "healthState=" + healthState +
                ", details=" + details +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder().withHealthState(healthState).withDetails(details);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private HealthState healthState;
        private Map<String, Object> details;

        private Builder() {
        }

        public Builder withHealthState(HealthState healthState) {
            this.healthState = healthState;
            return this;
        }

        public Builder withDetails(Map<String, Object> details) {
            this.details = details;
            return this;
        }

        public HealthStatus build() {
            return new HealthStatus(healthState, details);
        }
    }
}
