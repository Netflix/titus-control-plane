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

package com.netflix.titus.master.service.management;

/**
 * {@link CapacityManagementConfiguration} is complex enough, to be worth having Java Bean implementation
 * for testing purposes.
 */
public class BeanCapacityManagementConfiguration implements CapacityManagementConfiguration {

    private final ResourceDimensionConfiguration defaultApplicationResourceDimension;
    private final int defaultApplicationInstanceCount;
    private final long availableCapacityUpdateIntervalMs;
    private final double criticalTierBuffer;
    private final double flexTierBuffer;

    public BeanCapacityManagementConfiguration(Builder builder) {
        this.defaultApplicationResourceDimension = builder.defaultApplicationResourceDimension;
        this.defaultApplicationInstanceCount = builder.defaultApplicationInstanceCount;
        this.availableCapacityUpdateIntervalMs = builder.availableCapacityUpdateIntervalMs;
        this.criticalTierBuffer = builder.criticalTierBuffer;
        this.flexTierBuffer = builder.flexTierBuffer;
    }

    @Override
    public ResourceDimensionConfiguration getDefaultApplicationResourceDimension() {
        return defaultApplicationResourceDimension;
    }

    @Override
    public int getDefaultApplicationInstanceCount() {
        return defaultApplicationInstanceCount;
    }

    @Override
    public long getAvailableCapacityUpdateIntervalMs() {
        return availableCapacityUpdateIntervalMs;
    }

    @Override
    public double getCriticalTierBuffer() {
        return criticalTierBuffer;
    }

    @Override
    public double getFlexTierBuffer() {
        return flexTierBuffer;
    }

    @Override
    public String getDefaultSchedulerName() {
        return "fenzo";
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {

        private ResourceDimensionConfiguration defaultApplicationResourceDimension;
        private int defaultApplicationInstanceCount;
        private long availableCapacityUpdateIntervalMs;
        private double criticalTierBuffer;
        private double flexTierBuffer;

        private Builder() {
        }

        public Builder withInstanceType(String name, int minSize) {
            return this;
        }

        public Builder withDefaultApplicationResourceDimension(ResourceDimensionConfiguration defaultApplicationResourceDimension) {
            this.defaultApplicationResourceDimension = defaultApplicationResourceDimension;
            return this;
        }

        public Builder withDefaultApplicationInstanceCount(int defaultApplicationInstanceCount) {
            this.defaultApplicationInstanceCount = defaultApplicationInstanceCount;
            return this;
        }

        public Builder withAvailableCapacityUpdateIntervalMs(int availableCapacityUpdateIntervalMs) {
            this.availableCapacityUpdateIntervalMs = availableCapacityUpdateIntervalMs;
            return this;
        }

        public Builder withCriticalTierBuffer(double criticalTierBuffer) {
            this.criticalTierBuffer = criticalTierBuffer;
            return this;
        }

        public Builder withFlexTierBuffer(double flexTierBuffer) {
            this.flexTierBuffer = flexTierBuffer;
            return this;
        }

        public BeanCapacityManagementConfiguration build() {
            return new BeanCapacityManagementConfiguration(this);
        }
    }
}
