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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;

import io.netflix.titus.api.model.ResourceDimension;
import io.netflix.titus.api.model.Tier;
import io.netflix.titus.master.agent.ServerInfo;
import io.netflix.titus.master.agent.service.server.ServerInfoResolver;
import io.netflix.titus.master.model.ResourceDimensions;
import io.netflix.titus.master.service.management.CapacityGuaranteeStrategy;
import io.netflix.titus.master.service.management.CapacityManagementConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple strategy, where all resources required for a tier are first summed up, and next allocated on a first
 * instance type. If the instance count limit is reached, the next instance is used for allocation, and so on.
 * The remaining available instances for the tier are requested to be scaled to their configured min levels.
 */
@Singleton
public class SimpleCapacityGuaranteeStrategy implements CapacityGuaranteeStrategy {

    private static final Logger logger = LoggerFactory.getLogger(SimpleCapacityGuaranteeStrategy.class);

    private final CapacityManagementConfiguration configuration;
    private final ServerInfoResolver serverInfoResolver;

    @Inject
    public SimpleCapacityGuaranteeStrategy(CapacityManagementConfiguration configuration,
                                           ServerInfoResolver serverInfoResolver) {
        this.configuration = configuration;
        this.serverInfoResolver = serverInfoResolver;
    }

    @Override
    public CapacityAllocations compute(CapacityRequirements capacityRequirements) {
        Map<String, Integer> instanceAllocations = new HashMap<>();
        Map<Tier, ResourceDimension> resourceShortage = new HashMap<>();
        for (Tier tier : capacityRequirements.getTiers()) {
            // In Flex tier we have always 'DEFAULT' app, and if it is the only one, we should not scale the cluster
            // For other tiers we stop scaling, if there are no application SLAs configured
            boolean hasEnoughApps = (tier == Tier.Flex && capacityRequirements.getTierRequirements(tier).size() > 1)
                    || (tier != Tier.Flex && capacityRequirements.getTierRequirements(tier).size() > 0);
            if (hasEnoughApps) {
                Optional<ResourceDimension> left = allocate(tier, capacityRequirements, instanceAllocations);
                if (left.isPresent()) {
                    resourceShortage.put(tier, left.get());
                }
            }
        }
        return new CapacityAllocations(applyMinSizeLimits(instanceAllocations, capacityRequirements), resourceShortage);
    }

    private Optional<ResourceDimension> allocate(Tier tier, CapacityRequirements capacityRequirements, Map<String, Integer> instanceAllocations) {
        ResourceDimension left = computeTierResourceDimension(tier, capacityRequirements);

        List<String> instanceTypes = ConfigUtil.getTierInstanceTypes(tier, configuration);
        for (int i = 0; i < instanceTypes.size(); i++) {
            String instanceType = instanceTypes.get(i);
            Optional<ServerInfo> agentServerInfo = serverInfoResolver.resolve(instanceType);
            if (!agentServerInfo.isPresent()) {
                logger.warn("Unrecognized server type {}; ignoring it", instanceType);
                continue;
            }
            ResourceDimension instanceResources = ResourceDimensions.fromServerInfo(agentServerInfo.get());

            int instancesUsed = getInstancesUsed(instanceAllocations, instanceType);
            int instancesAvailable = capacityRequirements.getLimit(instanceType).getMaxSize() - instancesUsed;

            if (instancesAvailable > 0) {
                int instanceRequired = ResourceDimensions.divideAndRoundUp(left, instanceResources);
                if (instanceRequired <= instancesAvailable) {
                    instanceAllocations.put(instanceType, instancesUsed + instanceRequired);
                    // Make sure all instance types in a tier have value defined
                    while (++i < instanceTypes.size()) {
                        getInstancesUsed(instanceAllocations, instanceTypes.get(i));
                    }
                    return Optional.empty();
                }
                instanceAllocations.put(instanceType, instancesUsed + instancesAvailable);
                left = ResourceDimensions.subtractPositive(left, ResourceDimensions.multiply(instanceResources, instancesAvailable));
            }
        }
        logger.warn("Insufficient server resources available for tier {}. Lacking capacity for {}", tier, left);
        return Optional.of(left);
    }

    /**
     * Enforce min of min constraint (if calculated min level is lower than configured min, enforce the latter).
     */
    private Map<String, Integer> applyMinSizeLimits(Map<String, Integer> instanceAllocations,
                                                    CapacityRequirements capacityRequirements) {
        Map<String, Integer> result = new HashMap<>();
        for (Map.Entry<String, Integer> entry : instanceAllocations.entrySet()) {
            String instanceType = entry.getKey();
            int minSize = capacityRequirements.getLimit(instanceType).getMinSize();
            result.put(instanceType, Math.max(entry.getValue(), minSize));
        }
        return result;
    }

    private ResourceDimension computeTierResourceDimension(Tier tier, CapacityRequirements capacityRequirements) {
        ResourceDimension total = capacityRequirements.getTierRequirements(tier).stream()
                .map(sla -> ResourceDimensions.multiply(sla.getResourceDimension(), sla.getInstanceCount()))
                .reduce(ResourceDimension.empty(), (acc, item) -> ResourceDimensions.add(acc, item));

        double buffer = ConfigUtil.getTierBuffer(tier, configuration);
        return ResourceDimensions.multiply(total, 1 + buffer);
    }

    private int getInstancesUsed(Map<String, Integer> instanceAllocations, String instanceType) {
        int instancesUsed;
        if (!instanceAllocations.containsKey(instanceType)) {
            instanceAllocations.put(instanceType, instancesUsed = 0);
        } else {
            instancesUsed = instanceAllocations.get(instanceType);
        }
        return instancesUsed;
    }
}
