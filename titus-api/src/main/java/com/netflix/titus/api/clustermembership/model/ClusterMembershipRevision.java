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

package com.netflix.titus.api.clustermembership.model;

import java.util.Objects;

public class ClusterMembershipRevision {

    private final ClusterMember current;
    private final String code;
    private final String message;
    private final long timestamp;

    public ClusterMembershipRevision(ClusterMember current,
                                     String code,
                                     String message,
                                     long timestamp) {
        this.current = current;
        this.code = code;
        this.message = message;
        this.timestamp = timestamp;
    }

    public ClusterMember getCurrent() {
        return current;
    }

    public String getCode() {
        return code;
    }

    public String getMessage() {
        return message;
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
        ClusterMembershipRevision that = (ClusterMembershipRevision) o;
        return timestamp == that.timestamp &&
                Objects.equals(current, that.current) &&
                Objects.equals(code, that.code) &&
                Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(current, code, message, timestamp);
    }

    @Override
    public String toString() {
        return "ClusterMembershipRevision{" +
                "current=" + current +
                ", code='" + code + '\'' +
                ", message='" + message + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder().withCurrent(current).withCode(code).withMessage(message).withTimestamp(timestamp);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private ClusterMember current;
        private String code;
        private String message;
        private long timestamp;

        private Builder() {
        }

        public Builder withCurrent(ClusterMember current) {
            this.current = current;
            return this;
        }

        public Builder withCode(String code) {
            this.code = code;
            return this;
        }

        public Builder withMessage(String message) {
            this.message = message;
            return this;
        }

        public Builder withTimestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public ClusterMembershipRevision build() {
            return new ClusterMembershipRevision(current, code, message, timestamp);
        }
    }
}
