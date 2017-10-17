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

import java.util.Map;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.master.capacityManagement")
public interface CapacityManagementConfiguration {

    String DEFAULT_INSTANCE_TYPE = "DEFAULT";

    interface ResourceDimensionConfiguration {
        @DefaultValue("2")
        double getCPU();

        @DefaultValue("4096")
        int getMemoryMB();

        @DefaultValue("1024")
        int getDiskMB();

        @DefaultValue("128")
        int getNetworkMbs();
    }

    interface InstanceTypeConfig {
        /**
         * Instance type name.
         */
        String name();

        /**
         * Minimum server group size.
         */
        int minSize();
    }

    interface TierConfig {
        /**
         * AWS instance types that can be used by this tier.
         */
        String[] getInstanceTypes();

        /**
         * Additional resource allocation (ratio) on top of the requested amount.
         * For example, given instance count 100 and buffer 0.1, the actual allocation amount will be 110.
         */
        double getBuffer();
    }

    /**
     * Instance types configuration. The {@link #DEFAULT_INSTANCE_TYPE} name is reserved, and is used to set
     * a default configuration.
     */
    Map<String, InstanceTypeConfig> getInstanceTypes();

    /**
     * Configuration for tiers. A position in the list denotes tier priority level.
     */
    Map<String, TierConfig> getTiers();

    /**
     * If 'default' application is not defined, it will be transparently created with the configured resource
     * dimensions.
     */
    ResourceDimensionConfiguration getDefaultApplicationResourceDimension();

    /**
     * If 'DEFAULT' application is not defined, it will be transparently created with the configured instance count.
     */
    @DefaultValue("10")
    int getDefaultApplicationInstanceCount();

    /**
     * Interval at which information about available agent capacity is updated.
     */
    @DefaultValue("30000")
    long getAvailableCapacityUpdateIntervalMs();
}
