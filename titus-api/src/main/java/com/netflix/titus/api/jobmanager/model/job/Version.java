/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.api.jobmanager.model.job;

import java.util.Objects;

/**
 * TODO Add monotonically increasing `revision` number unique within a cell
 */
public class Version {

    private static final Version UNDEFINED = new Version(-1);

    private final long timestamp;

    public Version(long timestamp) {
        this.timestamp = timestamp;
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
        Version version = (Version) o;
        return timestamp == version.timestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp);
    }

    @Override
    public String toString() {
        return "Version{" +
                "timestamp=" + timestamp +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder().withTimestamp(timestamp);
    }

    public static Version undefined() {
        return UNDEFINED;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private long timestamp;

        private Builder() {
        }

        public Builder withTimestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Version build() {
            return new Version(timestamp);
        }
    }
}
