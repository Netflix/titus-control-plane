/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.service.management;

import java.util.HashMap;
import java.util.Map;

/**
 * {@link CapacityManagementConfiguration} is complex enough, to be worth having Java Bean implementation
 * for testing purposes.
 */
public class BeanCapacityManagementConfiguration implements CapacityManagementConfiguration {

    private final Map<String, InstanceTypeConfig> instanceTypes;
    private final Map<String, TierConfig> tiers;
    private final ResourceDimensionConfiguration defaultApplicationResourceDimension;
    private final int defaultApplicationInstanceCount;
    private final long availableCapacityUpdateIntervalMs;

    public BeanCapacityManagementConfiguration(Builder builder) {
        this.instanceTypes = builder.instanceTypes;
        this.tiers = builder.tiers;
        this.defaultApplicationResourceDimension = builder.defaultApplicationResourceDimension;
        this.defaultApplicationInstanceCount = builder.defaultApplicationInstanceCount;
        this.availableCapacityUpdateIntervalMs = builder.availableCapacityUpdateIntervalMs;
    }

    @Override
    public Map<String, InstanceTypeConfig> getInstanceTypes() {
        return instanceTypes;
    }

    @Override
    public Map<String, TierConfig> getTiers() {
        return tiers;
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

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {

        private Map<String, InstanceTypeConfig> instanceTypes = new HashMap<>();
        private Map<String, TierConfig> tiers = new HashMap<>();
        private ResourceDimensionConfiguration defaultApplicationResourceDimension;
        private int defaultApplicationInstanceCount;
        private long availableCapacityUpdateIntervalMs;

        private Builder() {
        }

        public Builder withInstanceType(String name, int minSize) {
            instanceTypes.put(name, new BeanInstanceTypeConfig(name, minSize));
            return this;
        }

        public Builder withCriticalTier(BeanTierConfig.Builder tierConfig) {
            tiers.put("0", tierConfig.build());
            return this;
        }

        public Builder withFlexTier(BeanTierConfig.Builder tierConfig) {
            tiers.put("1", tierConfig.build());
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

        public BeanCapacityManagementConfiguration build() {
            return new BeanCapacityManagementConfiguration(this);
        }
    }
}
