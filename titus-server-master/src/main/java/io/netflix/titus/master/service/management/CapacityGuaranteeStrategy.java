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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.netflix.titus.api.model.ApplicationSLA;
import io.netflix.titus.api.model.ResourceDimension;
import io.netflix.titus.api.model.Tier;

/**
 * Compute required number of instances per tier. As a tier may include multiple server types, with different amount
 * of resources, the result is a map of server type to its corresponding instance count. A single instance type
 * can be shared across many tiers, and the result is a sum of instances from all the tiers.
 * The capacity is not computed if there are no applications defined in a tier (vs zeroing all cluster sizes).
 * The {@link Tier#Flex} is not scaled if only one application is left, which is assumed to be the default application.
 */
public interface CapacityGuaranteeStrategy {

    class InstanceTypeLimit {
        private final int minSize;
        private final int maxSize;

        public InstanceTypeLimit(int minSize, int maxSize) {
            this.minSize = minSize;
            this.maxSize = maxSize;
        }

        public int getMinSize() {
            return minSize;
        }

        public int getMaxSize() {
            return maxSize;
        }
    }

    class CapacityRequirements {

        private final Map<Tier, List<ApplicationSLA>> tierRequirements;
        private final Map<String, InstanceTypeLimit> instanceTypeLimits;

        public CapacityRequirements(Map<Tier, List<ApplicationSLA>> tierRequirements, Map<String, InstanceTypeLimit> instanceTypeLimits) {
            this.tierRequirements = tierRequirements;
            this.instanceTypeLimits = instanceTypeLimits;
        }

        public Set<Tier> getTiers() {
            return tierRequirements.keySet();
        }

        public List<ApplicationSLA> getTierRequirements(Tier tier) {
            return tierRequirements.get(tier);
        }

        public InstanceTypeLimit getLimit(String instanceType) {
            return instanceTypeLimits.get(instanceType);
        }
    }

    class CapacityAllocations {

        private final Map<String, Integer> instanceAllocations;
        private final Map<Tier, ResourceDimension> resourceShortage;

        public CapacityAllocations(Map<String, Integer> instanceAllocations, Map<Tier, ResourceDimension> resourceShortage) {
            this.instanceAllocations = instanceAllocations;
            this.resourceShortage = resourceShortage;
        }

        public Set<String> getInstanceTypes() {
            return instanceAllocations.keySet();
        }

        public int getExpectedMinSize(String instanceType) {
            return instanceAllocations.get(instanceType);
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
