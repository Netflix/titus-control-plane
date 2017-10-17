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

package io.netflix.titus.master.service.management.internal;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import io.netflix.titus.api.model.Tier;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.master.service.management.CapacityManagementConfiguration;
import io.netflix.titus.master.service.management.CapacityManagementConfiguration.InstanceTypeConfig;
import io.netflix.titus.master.service.management.CapacityManagementConfiguration.TierConfig;

import static io.netflix.titus.common.util.CollectionsExt.isNullOrEmpty;
import static io.netflix.titus.master.service.management.CapacityManagementConfiguration.DEFAULT_INSTANCE_TYPE;
import static java.util.Arrays.asList;

/**
 * A collection of functions to process configuration data from {@link CapacityManagementConfiguration}.
 */
public class ConfigUtil {
    public static List<String> getTierInstanceTypes(Tier tier, CapacityManagementConfiguration configuration) {
        return asList(getTierConfigSafely(tier, configuration).getInstanceTypes());
    }

    public static double getTierBuffer(Tier tier, CapacityManagementConfiguration configuration) {
        return getTierConfigSafely(tier, configuration).getBuffer();
    }

    public static List<String> getAllInstanceTypes(CapacityManagementConfiguration configuration) {
        Map<String, TierConfig> tiers = configuration.getTiers();
        Set<String> instanceTypes = new HashSet<>();

        for (Tier tier : Tier.values()) {
            TierConfig tierConfig = tiers.get(Integer.toString(tier.ordinal()));
            if (tierConfig != null) {
                String[] tierTypes = tierConfig.getInstanceTypes();
                if (!isNullOrEmpty(tierTypes)) {
                    CollectionsExt.addAll(instanceTypes, tierTypes);
                } else {
                    throw new IllegalStateException(tier + " has no instance type defined");
                }
            } else {
                throw new IllegalStateException("Missing configuration for tier " + tier);
            }
        }

        return new ArrayList<>(instanceTypes);
    }

    private static TierConfig getTierConfigSafely(Tier tier, CapacityManagementConfiguration configuration) {
        Map<String, TierConfig> tiers = configuration.getTiers();
        Preconditions.checkNotNull(tiers, "Tiers configuration not defined");

        TierConfig tierConfig = tiers.get(Integer.toString(tier.ordinal()));
        Preconditions.checkNotNull(tierConfig, tier + " configuration not defined");
        Preconditions.checkArgument(!isNullOrEmpty(tierConfig.getInstanceTypes()), tier + " has no instance ");

        return tierConfig;
    }

    public static int getInstanceTypeMinSize(CapacityManagementConfiguration configuration,
                                             String instanceType) {
        if (configuration.getInstanceTypes() == null) {
            return 0;
        }
        int defaultValue = 0;
        for (int idx = 0; true; idx++) {
            InstanceTypeConfig instanceTypeConfig = configuration.getInstanceTypes().get(Integer.toString(idx));
            if (instanceTypeConfig == null || instanceTypeConfig.name() == null) {
                return defaultValue;
            }
            if (DEFAULT_INSTANCE_TYPE.equals(instanceTypeConfig.name())) {
                defaultValue = instanceTypeConfig.minSize();
            }
            if (instanceType.equals(instanceTypeConfig.name())) {
                return instanceTypeConfig.minSize();
            }
        }
    }
}
