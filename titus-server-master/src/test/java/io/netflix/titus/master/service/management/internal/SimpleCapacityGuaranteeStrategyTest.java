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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.netflix.titus.api.model.ApplicationSLA;
import io.netflix.titus.api.model.ResourceDimension;
import io.netflix.titus.api.model.Tier;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.master.agent.service.server.ServerInfoResolvers;
import io.netflix.titus.master.endpoint.v2.rest.ApplicationSlaManagementEndpoint;
import io.netflix.titus.master.service.management.BeanCapacityManagementConfiguration;
import io.netflix.titus.master.service.management.BeanTierConfig;
import io.netflix.titus.master.service.management.CapacityGuaranteeStrategy;
import io.netflix.titus.master.service.management.CapacityGuaranteeStrategy.CapacityAllocations;
import io.netflix.titus.master.service.management.CapacityGuaranteeStrategy.CapacityRequirements;
import io.netflix.titus.master.service.management.CapacityGuaranteeStrategy.InstanceTypeLimit;
import io.netflix.titus.master.service.management.CapacityManagementConfiguration;
import io.netflix.titus.testkit.data.core.ApplicationSlaSample;
import org.junit.Test;

import static io.netflix.titus.common.aws.AwsInstanceType.G2_8XLARGE_ID;
import static io.netflix.titus.common.aws.AwsInstanceType.M4_2XLARGE_ID;
import static io.netflix.titus.common.aws.AwsInstanceType.M4_XLARGE_ID;
import static io.netflix.titus.common.aws.AwsInstanceType.R3_4XLARGE_ID;
import static io.netflix.titus.common.aws.AwsInstanceType.R3_8XLARGE_ID;
import static io.netflix.titus.common.util.CollectionsExt.asSet;
import static io.netflix.titus.common.util.CollectionsExt.merge;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;

public class SimpleCapacityGuaranteeStrategyTest {

    private static Set<String> CRITICAL_INSTANCE_TYPES = asSet(M4_XLARGE_ID, M4_2XLARGE_ID, G2_8XLARGE_ID);
    private static Set<String> FLEX_INSTANCE_TYPES = asSet(R3_8XLARGE_ID, R3_4XLARGE_ID, G2_8XLARGE_ID);
    private static Set<String> ALL_INSTANCE_TYPES = merge(CRITICAL_INSTANCE_TYPES, FLEX_INSTANCE_TYPES);

    private static final CapacityManagementConfiguration CONFIGURATION = BeanCapacityManagementConfiguration.newBuilder()
            .withCriticalTier(BeanTierConfig.newBuilder()
                    .withBuffer(0.1)
                    .withInstanceTypes(M4_XLARGE_ID, M4_2XLARGE_ID, G2_8XLARGE_ID)
            )
            .withFlexTier(BeanTierConfig.newBuilder()
                    .withBuffer(0.1)
                    .withInstanceTypes(R3_8XLARGE_ID, R3_4XLARGE_ID, G2_8XLARGE_ID)
            )
            .build();

    private static final InstanceTypeLimit DEFAULT_INSTANCE_TYPE_LIMIT = new InstanceTypeLimit(0, 1);
    private static final Map<String, InstanceTypeLimit> DEFAULT_LIMITS = CollectionsExt.<String, InstanceTypeLimit>newHashMap()
            .entry(M4_XLARGE_ID, DEFAULT_INSTANCE_TYPE_LIMIT)
            .entry(M4_2XLARGE_ID, DEFAULT_INSTANCE_TYPE_LIMIT)
            .entry(R3_4XLARGE_ID, DEFAULT_INSTANCE_TYPE_LIMIT)
            .entry(R3_8XLARGE_ID, DEFAULT_INSTANCE_TYPE_LIMIT)
            .entry(G2_8XLARGE_ID, DEFAULT_INSTANCE_TYPE_LIMIT)
            .toMap();

    private final CapacityGuaranteeStrategy strategy = new SimpleCapacityGuaranteeStrategy(
            CONFIGURATION,
            ServerInfoResolvers.fromAwsInstanceTypes()
    );

    @Test
    public void testSingleInstanceFitsAll() throws Exception {
        ApplicationSLA smallSLA = ApplicationSlaSample.CriticalSmall.builder().withInstanceCount(1).build();
        CapacityRequirements requirements = new CapacityRequirements(
                singletonMap(Tier.Critical, singletonList(smallSLA)),
                updateLimit(M4_XLARGE_ID, 2)
        );
        CapacityAllocations allocations = strategy.compute(requirements);

        assertThat(allocations.getInstanceTypes()).containsOnlyElementsOf(CRITICAL_INSTANCE_TYPES);
        assertThat(allocations.getExpectedMinSize(M4_XLARGE_ID)).isEqualTo(1);
        assertThat(allocations.getExpectedMinSize(M4_2XLARGE_ID)).isEqualTo(0);
        assertThat(allocations.getExpectedMinSize(G2_8XLARGE_ID)).isEqualTo(0);
    }

    @Test
    public void testLargeAllocation() throws Exception {
        ApplicationSLA largeSLA = ApplicationSlaSample.CriticalLarge.builder().withInstanceCount(10).build(); // 2 * 10 CPUs
        CapacityRequirements requirements = new CapacityRequirements(
                singletonMap(Tier.Critical, singletonList(largeSLA)),
                updateLimit(M4_XLARGE_ID, 10) // 4 CPUs
        );
        CapacityAllocations allocations = strategy.compute(requirements);

        assertThat(allocations.getInstanceTypes()).containsOnlyElementsOf(CRITICAL_INSTANCE_TYPES);
        assertThat(allocations.getExpectedMinSize(M4_XLARGE_ID)).isEqualTo(6);
        assertThat(allocations.getExpectedMinSize(M4_2XLARGE_ID)).isEqualTo(0);
        assertThat(allocations.getExpectedMinSize(G2_8XLARGE_ID)).isEqualTo(0);
    }

    @Test
    public void testFlexTierMultiInstanceAllocation() throws Exception {
        ApplicationSLA defaultSLA = ApplicationSLA.newBuilder()
                .withAppName(ApplicationSlaManagementEndpoint.DEFAULT_APPLICATION)
                .withTier(Tier.Flex)
                .withResourceDimension(ResourceDimension
                        .newBuilder().withCpus(4).withMemoryMB(512).withNetworkMbs(128).withDiskMB(512).build()
                )
                .withInstanceCount(10)
                .build();
        ApplicationSLA flexAppSLA = ApplicationSLA.newBuilder(defaultSLA).withInstanceCount(40).build();

        Map<String, InstanceTypeLimit> instanceTypeLimits = CollectionsExt.newHashMap(DEFAULT_LIMITS)
                .entry(R3_8XLARGE_ID, new InstanceTypeLimit(0, 5)) // 32 CPUs
                .entry(R3_4XLARGE_ID, new InstanceTypeLimit(0, 5)) // 16 CPUs
                .entry(G2_8XLARGE_ID, new InstanceTypeLimit(0, 5)) // 32 CPUs
                .toMap();

        CapacityRequirements requirements = new CapacityRequirements(
                singletonMap(Tier.Flex, asList(defaultSLA, flexAppSLA)),
                instanceTypeLimits
        );

        CapacityAllocations allocations = strategy.compute(requirements);

        assertThat(allocations.getInstanceTypes()).containsOnlyElementsOf(FLEX_INSTANCE_TYPES);
        assertThat(allocations.getExpectedMinSize(R3_8XLARGE_ID)).isEqualTo(5);
        assertThat(allocations.getExpectedMinSize(R3_4XLARGE_ID)).isEqualTo(4);
        assertThat(allocations.getExpectedMinSize(G2_8XLARGE_ID)).isEqualTo(0);
    }

    @Test
    public void testTierSharedInstanceAllocation() throws Exception {
        ApplicationSLA criticalSLA = ApplicationSlaSample.CriticalSmall.builder().withInstanceCount(20).build(); // 1 * 20 CPUs
        ApplicationSLA flexSLA = ApplicationSlaSample.FlexSmall.builder().withInstanceCount(20).build(); // 1 * 20 CPUs
        ApplicationSLA defaultFlexSLA = ApplicationSlaSample.DefaultFlex.builder().withInstanceCount(1).build(); // 2 * 1 CPUs

        Map<String, InstanceTypeLimit> instanceTypeLimits = CollectionsExt.<String, InstanceTypeLimit>newHashMap()
                .entry(M4_XLARGE_ID, new InstanceTypeLimit(0, 0))
                .entry(M4_2XLARGE_ID, new InstanceTypeLimit(0, 0))
                .entry(R3_4XLARGE_ID, new InstanceTypeLimit(0, 0))
                .entry(R3_8XLARGE_ID, new InstanceTypeLimit(0, 0))
                .entry(G2_8XLARGE_ID, new InstanceTypeLimit(0, 3)) // 32 CPUs
                .toMap();

        Map<Tier, List<ApplicationSLA>> tierSLAs = CollectionsExt.<Tier, List<ApplicationSLA>>newHashMap()
                .entry(Tier.Critical, singletonList(criticalSLA))
                .entry(Tier.Flex, asList(defaultFlexSLA, flexSLA))
                .toMap();

        CapacityRequirements requirements = new CapacityRequirements(tierSLAs, instanceTypeLimits);

        CapacityAllocations allocations = strategy.compute(requirements);

        assertThat(allocations.getInstanceTypes()).containsOnlyElementsOf(ALL_INSTANCE_TYPES);
        assertThat(allocations.getExpectedMinSize(G2_8XLARGE_ID)).isEqualTo(2);
    }

    @Test
    public void testBuffer() throws Exception {
        ApplicationSLA smallSLA = ApplicationSlaSample.CriticalSmall.builder().withInstanceCount(4).build(); // 4 CPUs
        CapacityRequirements requirements = new CapacityRequirements(
                singletonMap(Tier.Critical, singletonList(smallSLA)),
                updateLimit(M4_XLARGE_ID, 2)
        );

        // Our SLA allocation requires 1 instance, but buffer goes over it, so we require 2
        CapacityAllocations allocations = strategy.compute(requirements);

        assertThat(allocations.getInstanceTypes()).containsOnlyElementsOf(CRITICAL_INSTANCE_TYPES);
        assertThat(allocations.getExpectedMinSize(M4_XLARGE_ID)).isEqualTo(2);

    }

    @Test
    public void testAllocationNotChangedIfNoAppInTier() throws Exception {
        CapacityRequirements requirements = new CapacityRequirements(
                Collections.emptyMap(),
                DEFAULT_LIMITS
        );
        CapacityAllocations allocations = strategy.compute(requirements);
        assertThat(allocations.getInstanceTypes()).isEmpty();
    }

    @Test
    public void testAllocationNotChangedIfOnlyDefaultAppInFlexTier() throws Exception {
        CapacityRequirements requirements = new CapacityRequirements(
                Collections.singletonMap(Tier.Flex, singletonList(ApplicationSlaSample.DefaultFlex.build())),
                DEFAULT_LIMITS
        );
        CapacityAllocations allocations = strategy.compute(requirements);
        assertThat(allocations.getInstanceTypes()).isEmpty();
    }

    @Test
    public void testAllocationsMinLimitsAreApplied() throws Exception {
        ApplicationSLA smallSLA = ApplicationSlaSample.CriticalSmall.builder().withInstanceCount(1).build();

        Map<String, InstanceTypeLimit> limits = CollectionsExt.newHashMap(DEFAULT_LIMITS)
                .entry(M4_XLARGE_ID, new InstanceTypeLimit(2, 10))
                .entry(M4_2XLARGE_ID, new InstanceTypeLimit(3, 10))
                .entry(G2_8XLARGE_ID, new InstanceTypeLimit(5, 10))
                .toMap();

        CapacityRequirements requirements = new CapacityRequirements(
                singletonMap(Tier.Critical, singletonList(smallSLA)),
                limits
        );
        CapacityAllocations allocations = strategy.compute(requirements);

        assertThat(allocations.getInstanceTypes()).containsOnlyElementsOf(CRITICAL_INSTANCE_TYPES);
        assertThat(allocations.getExpectedMinSize(M4_XLARGE_ID)).isEqualTo(2);
        assertThat(allocations.getExpectedMinSize(M4_2XLARGE_ID)).isEqualTo(3);
        assertThat(allocations.getExpectedMinSize(G2_8XLARGE_ID)).isEqualTo(5);
    }

    @Test
    public void testResourceShortageReporting() throws Exception {
        ApplicationSLA veryBigSLA = ApplicationSlaSample.CriticalSmall.builder().withInstanceCount(100).build();
        CapacityRequirements requirements = new CapacityRequirements(
                singletonMap(Tier.Critical, singletonList(veryBigSLA)),
                updateLimit(M4_XLARGE_ID, 2)
        );

        CapacityAllocations allocations = strategy.compute(requirements);
        assertThat(allocations.getTiersWithResourceShortage()).containsExactly(Tier.Critical);

    }

    private Map<String, InstanceTypeLimit> updateLimit(String instanceType, int maxLimit) {
        return CollectionsExt.newHashMap(DEFAULT_LIMITS).entry(instanceType, new InstanceTypeLimit(0, maxLimit)).toMap();
    }
}