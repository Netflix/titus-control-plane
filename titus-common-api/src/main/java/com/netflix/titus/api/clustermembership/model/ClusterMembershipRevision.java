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

public class ClusterMembershipRevision<T> {

    private final T current;
    private final String code;
    private final String message;
    private final long revision;
    private final long timestamp;

    public ClusterMembershipRevision(T current,
                                     String code,
                                     String message,
                                     long revision,
                                     long timestamp) {
        this.current = current;
        this.code = code;
        this.message = message;
        this.revision = revision;
        this.timestamp = timestamp;
    }

    public T getCurrent() {
        return current;
    }

    public String getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public long getRevision() {
        return revision;
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
        ClusterMembershipRevision revision1 = (ClusterMembershipRevision) o;
        return revision == revision1.revision &&
                timestamp == revision1.timestamp &&
                Objects.equals(current, revision1.current) &&
                Objects.equals(code, revision1.code) &&
                Objects.equals(message, revision1.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(current, code, message, revision, timestamp);
    }

    @Override
    public String toString() {
        return "ClusterMembershipRevision{" +
                "current=" + current +
                ", code='" + code + '\'' +
                ", message='" + message + '\'' +
                ", revision=" + revision +
                ", timestamp=" + timestamp +
                '}';
    }

    public Builder<T> toBuilder() {
        return ClusterMembershipRevision.<T>newBuilder().withCurrent(current).withCode(code).withMessage(message).withRevision(revision).withTimestamp(timestamp);
    }

    public static <T> Builder<T> newBuilder() {
        return new Builder<>();
    }

    public static final class Builder<T> {
        private T current;
        private String code;
        private String message;
        private long timestamp;
        private long revision;

        private Builder() {
        }

        public Builder<T> withCurrent(T current) {
            this.current = current;
            return this;
        }

        public Builder<T> withCode(String code) {
            this.code = code;
            return this;
        }

        public Builder<T> withMessage(String message) {
            this.message = message;
            return this;
        }

        public Builder<T> withTimestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder<T> withRevision(long revision) {
            this.revision = revision;
            return this;
        }

        public ClusterMembershipRevision<T> build() {
            return new ClusterMembershipRevision<>(current, code, message, revision, timestamp);
        }
    }
}
