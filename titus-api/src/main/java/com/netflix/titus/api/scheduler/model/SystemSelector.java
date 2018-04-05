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

package com.netflix.titus.api.scheduler.model;

import java.util.Objects;
import javax.validation.constraints.NotNull;

import com.netflix.titus.common.model.sanitizer.ClassInvariant;

/**
 * A system selector is a mechanism that allows rules to be evaluated in order to dynamically change scheduling behavior.
 */
@ClassInvariant(expr = "@asserts.validateSystemSelector(#this)")
public class SystemSelector {

    @NotNull
    private final String id;

    @NotNull
    private final String description;

    private final boolean enabled;

    private final int priority;

    @NotNull
    private final String reason;

    private final long timestamp;

    private final Should should;

    private final Must must;

    public SystemSelector(String id,
                          String description,
                          boolean enabled,
                          int priority,
                          String reason,
                          long timestamp,
                          Should should,
                          Must must) {
        this.id = id;
        this.description = description;
        this.enabled = enabled;
        this.priority = priority;
        this.reason = reason;
        this.timestamp = timestamp;
        this.should = should;
        this.must = must;
    }

    public String getId() {
        return id;
    }

    public String getDescription() {
        return description;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public int getPriority() {
        return priority;
    }

    public String getReason() {
        return reason;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Should getShould() {
        return should;
    }

    public Must getMust() {
        return must;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SystemSelector that = (SystemSelector) o;
        return enabled == that.enabled &&
                priority == that.priority &&
                timestamp == that.timestamp &&
                Objects.equals(id, that.id) &&
                Objects.equals(description, that.description) &&
                Objects.equals(reason, that.reason) &&
                Objects.equals(should, that.should) &&
                Objects.equals(must, that.must);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, description, enabled, priority, reason, timestamp, should, must);
    }

    @Override
    public String toString() {
        return "SystemSelector{" +
                "id='" + id + '\'' +
                ", description='" + description + '\'' +
                ", enabled=" + enabled +
                ", priority=" + priority +
                ", reason='" + reason + '\'' +
                ", timestamp=" + timestamp +
                ", should=" + should +
                ", must=" + must +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String id;
        private String description;
        private boolean enabled;
        private int priority;
        private String reason;
        private long timestamp;
        private Should should;
        private Must must;

        private Builder() {
        }

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public Builder withDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder withEnabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder withPriority(int priority) {
            this.priority = priority;
            return this;
        }

        public Builder withReason(String reason) {
            this.reason = reason;
            return this;
        }

        public Builder withTimestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder withShould(Should should) {
            this.should = should;
            return this;
        }

        public Builder withMust(Must must) {
            this.must = must;
            return this;
        }

        public Builder but() {
            return newBuilder().withId(id).withDescription(description).withEnabled(enabled).withPriority(priority)
                    .withReason(reason).withTimestamp(timestamp).withShould(should).withMust(must);
        }

        public SystemSelector build() {
            return new SystemSelector(id, description, enabled, priority, reason, timestamp, should, must);
        }
    }
}
