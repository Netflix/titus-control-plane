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

package com.netflix.titus.api.appscale.model;

public class AutoScalableTarget {
    private final String resourceId;
    private final int minCapacity;
    private final int maxCapacity;

    public AutoScalableTarget(String resourceId, int minCapacity, int maxCapacity) {
        this.resourceId = resourceId;
        this.minCapacity = minCapacity;
        this.maxCapacity = maxCapacity;
    }

    public String getResourceId() {
        return resourceId;
    }

    public int getMinCapacity() {
        return minCapacity;
    }

    public int getMaxCapacity() {
        return maxCapacity;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private String resourceId;
        private int minCapacity;
        private int maxCapacity;

        private Builder() {
        }

        public Builder withResourceId(String resourceId) {
            this.resourceId = resourceId;
            return this;
        }

        public Builder withMinCapacity(int minCapacity) {
            this.minCapacity = minCapacity;
            return this;
        }

        public Builder withMaxCapacity(int maxCapacity) {
            this.maxCapacity = maxCapacity;
            return this;
        }

        public AutoScalableTarget build() {
            return new AutoScalableTarget(this.resourceId, this.minCapacity, this.maxCapacity);
        }

    }

    @Override
    public String toString() {
        return "AutoScalableTarget{" +
                "resourceId='" + resourceId + '\'' +
                ", minCapacity=" + minCapacity +
                ", maxCapacity=" + maxCapacity +
                '}';
    }
}
