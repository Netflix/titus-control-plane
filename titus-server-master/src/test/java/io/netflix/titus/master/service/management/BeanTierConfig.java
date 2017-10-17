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

import java.util.Collection;

import io.netflix.titus.master.service.management.CapacityManagementConfiguration.TierConfig;

public class BeanTierConfig implements TierConfig {

    private final String[] instanceTypes;
    private final double buffer;

    public BeanTierConfig(Builder builder) {
        this.instanceTypes = builder.instanceTypes;
        this.buffer = builder.buffer;
    }

    @Override
    public String[] getInstanceTypes() {
        return instanceTypes;
    }

    @Override
    public double getBuffer() {
        return buffer;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String[] instanceTypes;
        private double buffer;

        private Builder() {
        }

        public Builder withInstanceTypes(String... instanceTypes) {
            this.instanceTypes = instanceTypes;
            return this;
        }

        public Builder withInstanceTypes(Collection<String> instanceTypes) {
            this.instanceTypes = instanceTypes.toArray(new String[instanceTypes.size()]);
            return this;
        }

        public Builder withBuffer(double buffer) {
            this.buffer = buffer;
            return this;
        }

        public BeanTierConfig build() {
            return new BeanTierConfig(this);
        }
    }
}
