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

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.master.capacityManagement")
public interface CapacityManagementConfiguration {

    interface ResourceDimensionConfiguration {
        @DefaultValue("2")
        double getCPU();

        @DefaultValue("4096")
        int getMemoryMB();

        @DefaultValue("10000")
        int getDiskMB();

        @DefaultValue("128")
        int getNetworkMbs();
    }

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

    /**
     * The buffer percentage size of the critical tier
     */
    @DefaultValue("0.05")
    double getCriticalTierBuffer();

    /**
     * The buffer percentage size of the flex tier
     */
    @DefaultValue("0.05")
    double getFlexTierBuffer();

    @DefaultValue("fenzo")
    String getDefaultSchedulerName();

}