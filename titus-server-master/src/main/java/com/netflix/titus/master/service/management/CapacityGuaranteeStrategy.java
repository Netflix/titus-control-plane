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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.Tier;

/**
 * Compute required number of instances per tier. As a tier may include multiple server types, with different amount
 * of resources, the result is a map of server type to its corresponding instance count. A single instance type
 * can be shared across many tiers, and the result is a sum of instances from all the tiers.
 * The capacity is not computed if there are no applications defined in a tier (vs zeroing all cluster sizes).
 * The {@link Tier#Flex} is not scaled if only one application is left, which is assumed to be the default application.
 */
public interface CapacityGuaranteeStrategy {
    class CapacityRequirements {

        private final Map<Tier, List<ApplicationSLA>> tierRequirements;

        public CapacityRequirements(Map<Tier, List<ApplicationSLA>> tierRequirements) {
            this.tierRequirements = tierRequirements;
        }

        public Set<Tier> getTiers() {
            return tierRequirements.keySet();
        }

        public List<ApplicationSLA> getTierRequirements(Tier tier) {
            return tierRequirements.get(tier);
        }
    }

    class CapacityAllocations {

        private final Map<AgentInstanceGroup, Integer> instanceAllocations;
        private final Map<Tier, ResourceDimension> resourceShortage;

        public CapacityAllocations(Map<AgentInstanceGroup, Integer> instanceAllocations, Map<Tier, ResourceDimension> resourceShortage) {
            this.instanceAllocations = instanceAllocations;
            this.resourceShortage = resourceShortage;
        }

        public Set<AgentInstanceGroup> getInstanceGroups() {
            return instanceAllocations.keySet();
        }

        public int getExpectedMinSize(AgentInstanceGroup instanceGroup) {
            return instanceAllocations.get(instanceGroup);
        }

        public Set<Tier> getTiersWithResourceShortage() {
            return Collections.unmodifiableSet(resourceShortage.keySet());
        }

        public ResourceDimension getResourceShortage(Tier tier) {
            return resourceShortage.get(tier);
        }
    }

    CapacityAllocations compute(CapacityRequirements capacityRequirements);
}