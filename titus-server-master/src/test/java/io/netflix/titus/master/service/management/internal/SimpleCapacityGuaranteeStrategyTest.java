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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.netflix.titus.api.agent.model.AgentInstanceGroup;
import io.netflix.titus.api.agent.model.AutoScaleRule;
import io.netflix.titus.api.agent.service.AgentManagementService;
import io.netflix.titus.api.model.ApplicationSLA;
import io.netflix.titus.api.model.ResourceDimension;
import io.netflix.titus.api.model.Tier;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.master.agent.service.server.ServerInfoResolvers;
import io.netflix.titus.master.endpoint.v2.rest.ApplicationSlaManagementEndpoint;
import io.netflix.titus.master.service.management.BeanCapacityManagementConfiguration;
import io.netflix.titus.master.service.management.CapacityGuaranteeStrategy;
import io.netflix.titus.master.service.management.CapacityGuaranteeStrategy.CapacityAllocations;
import io.netflix.titus.master.service.management.CapacityGuaranteeStrategy.CapacityRequirements;
import io.netflix.titus.master.service.management.CapacityManagementConfiguration;
import io.netflix.titus.testkit.data.core.ApplicationSlaSample;
import io.netflix.titus.testkit.model.agent.AgentGenerator;
import org.junit.Test;

import static io.netflix.titus.common.aws.AwsInstanceType.M4_4XLARGE_ID;
import static io.netflix.titus.common.aws.AwsInstanceType.M4_XLARGE_ID;
import static io.netflix.titus.common.aws.AwsInstanceType.P2_8XLARGE_ID;
import static io.netflix.titus.common.aws.AwsInstanceType.R4_4XLARGE_ID;
import static io.netflix.titus.common.aws.AwsInstanceType.R4_8XLARGE_ID;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SimpleCapacityGuaranteeStrategyTest {

    private static final CapacityManagementConfiguration configuration = BeanCapacityManagementConfiguration.newBuilder()
            .withCriticalTierBuffer(0.1)
            .withFlexTierBuffer(0.1)
            .build();

    private final AgentManagementService agentManagementService = mock(AgentManagementService.class);

    private final CapacityGuaranteeStrategy strategy = new SimpleCapacityGuaranteeStrategy(
            configuration,
            agentManagementService,
            ServerInfoResolvers.fromAwsInstanceTypes()
    );

    @Test
    public void testSingleInstanceFitsAll() {
        List<AgentInstanceGroup> instanceGroups = asList(
                getInstanceGroup(Tier.Critical, M4_XLARGE_ID, 0, 2),
                getInstanceGroup(Tier.Critical, M4_4XLARGE_ID, 0, 0),
                getInstanceGroup(Tier.Flex, R4_8XLARGE_ID, 0, 0),
                getInstanceGroup(Tier.Flex, P2_8XLARGE_ID, 0, 0)
        );
        when(agentManagementService.getInstanceGroups()).thenReturn(instanceGroups);

        ApplicationSLA smallSLA = ApplicationSlaSample.CriticalSmall.builder().withInstanceCount(1).build();
        CapacityRequirements requirements = new CapacityRequirements(singletonMap(Tier.Critical, singletonList(smallSLA)));
        CapacityAllocations allocations = strategy.compute(requirements);

        allocations.getInstanceGroups().forEach(instanceGroup -> {
            assertThat(instanceGroup.getTier()).isEqualTo(Tier.Critical);
        });

        AgentInstanceGroup m4xlInstanceGroup = findInstanceGroupByInstanceType(allocations.getInstanceGroups(), M4_XLARGE_ID);
        assertThat(allocations.getExpectedMinSize(m4xlInstanceGroup)).isEqualTo(1);

        AgentInstanceGroup m44xlInstanceGroup = findInstanceGroupByInstanceType(allocations.getInstanceGroups(), M4_4XLARGE_ID);
        assertThat(allocations.getExpectedMinSize(m44xlInstanceGroup)).isEqualTo(0);
    }

    @Test
    public void testLargeAllocation() {
        List<AgentInstanceGroup> instanceGroups = asList(
                getInstanceGroup(Tier.Critical, M4_XLARGE_ID, 0, 10),
                getInstanceGroup(Tier.Critical, M4_4XLARGE_ID, 0, 0),
                getInstanceGroup(Tier.Flex, R4_8XLARGE_ID, 0, 0),
                getInstanceGroup(Tier.Flex, P2_8XLARGE_ID, 0, 0)
        );
        when(agentManagementService.getInstanceGroups()).thenReturn(instanceGroups);

        ApplicationSLA largeSLA = ApplicationSlaSample.CriticalLarge.builder().withInstanceCount(10).build(); // 2 * 10 CPUs
        CapacityRequirements requirements = new CapacityRequirements(singletonMap(Tier.Critical, singletonList(largeSLA)));
        CapacityAllocations allocations = strategy.compute(requirements);

        allocations.getInstanceGroups().forEach(instanceGroup -> {
            assertThat(instanceGroup.getTier()).isEqualTo(Tier.Critical);
        });

        AgentInstanceGroup m4xlInstanceGroup = findInstanceGroupByInstanceType(allocations.getInstanceGroups(), M4_XLARGE_ID);
        assertThat(allocations.getExpectedMinSize(m4xlInstanceGroup)).isEqualTo(6);

        AgentInstanceGroup m44xlInstanceGroup = findInstanceGroupByInstanceType(allocations.getInstanceGroups(), M4_4XLARGE_ID);
        assertThat(allocations.getExpectedMinSize(m44xlInstanceGroup)).isEqualTo(0);
    }

    @Test
    public void testFlexTierMultiInstanceAllocation() {
        List<AgentInstanceGroup> instanceGroups = asList(
                getInstanceGroup(Tier.Critical, M4_XLARGE_ID, 0, 0),
                getInstanceGroup(Tier.Critical, M4_4XLARGE_ID, 0, 0),
                getInstanceGroup(Tier.Flex, R4_8XLARGE_ID, 0, 5),
                getInstanceGroup(Tier.Flex, R4_4XLARGE_ID, 0, 5),
                getInstanceGroup(Tier.Flex, P2_8XLARGE_ID, 0, 5)
        );
        when(agentManagementService.getInstanceGroups()).thenReturn(instanceGroups);

        ApplicationSLA defaultSLA = ApplicationSLA.newBuilder()
                .withAppName(ApplicationSlaManagementEndpoint.DEFAULT_APPLICATION)
                .withTier(Tier.Flex)
                .withResourceDimension(ResourceDimension
                        .newBuilder().withCpus(4).withMemoryMB(512).withNetworkMbs(128).withDiskMB(512).build()
                )
                .withInstanceCount(10)
                .build();
        ApplicationSLA flexAppSLA = ApplicationSLA.newBuilder(defaultSLA).withInstanceCount(40).build();

        CapacityRequirements requirements = new CapacityRequirements(singletonMap(Tier.Flex, asList(defaultSLA, flexAppSLA)));

        CapacityAllocations allocations = strategy.compute(requirements);

        allocations.getInstanceGroups().forEach(instanceGroup -> {
            assertThat(instanceGroup.getTier()).isEqualTo(Tier.Flex);
        });

        AgentInstanceGroup r48xlInstanceGroup = findInstanceGroupByInstanceType(allocations.getInstanceGroups(), R4_8XLARGE_ID);
        assertThat(allocations.getExpectedMinSize(r48xlInstanceGroup)).isEqualTo(5);

        AgentInstanceGroup r44xlInstanceGroup = findInstanceGroupByInstanceType(allocations.getInstanceGroups(), R4_4XLARGE_ID);
        assertThat(allocations.getExpectedMinSize(r44xlInstanceGroup)).isEqualTo(4);

        AgentInstanceGroup p28xlInstanceGroup = findInstanceGroupByInstanceType(allocations.getInstanceGroups(), P2_8XLARGE_ID);
        assertThat(allocations.getExpectedMinSize(p28xlInstanceGroup)).isEqualTo(0);
    }

    @Test
    public void testTierSharedInstanceAllocation() {
        List<AgentInstanceGroup> instanceGroups = asList(
                getInstanceGroup(Tier.Critical, M4_XLARGE_ID, 0, 0),
                getInstanceGroup(Tier.Critical, M4_4XLARGE_ID, 0, 0),
                getInstanceGroup(Tier.Flex, R4_4XLARGE_ID, 0, 0),
                getInstanceGroup(Tier.Flex, R4_8XLARGE_ID, 0, 0),
                getInstanceGroup(Tier.Flex, P2_8XLARGE_ID, 0, 3)
        );
        when(agentManagementService.getInstanceGroups()).thenReturn(instanceGroups);

        ApplicationSLA criticalSLA = ApplicationSlaSample.CriticalSmall.builder().withInstanceCount(20).build(); // 1 * 20 CPUs
        ApplicationSLA flexSLA = ApplicationSlaSample.FlexSmall.builder().withInstanceCount(20).build(); // 1 * 20 CPUs
        ApplicationSLA defaultFlexSLA = ApplicationSlaSample.DefaultFlex.builder().withInstanceCount(1).build(); // 2 * 1 CPUs

        Map<Tier, List<ApplicationSLA>> tierSLAs = CollectionsExt.<Tier, List<ApplicationSLA>>newHashMap()
                .entry(Tier.Critical, singletonList(criticalSLA))
                .entry(Tier.Flex, asList(defaultFlexSLA, flexSLA))
                .toMap();

        CapacityRequirements requirements = new CapacityRequirements(tierSLAs);

        CapacityAllocations allocations = strategy.compute(requirements);

        AgentInstanceGroup p28xlInstanceGroup = findInstanceGroupByInstanceType(allocations.getInstanceGroups(), P2_8XLARGE_ID);
        assertThat(allocations.getExpectedMinSize(p28xlInstanceGroup)).isEqualTo(1);
    }

    @Test
    public void testBuffer() {
        List<AgentInstanceGroup> instanceGroups = singletonList(
                getInstanceGroup(Tier.Critical, M4_XLARGE_ID, 0, 2)
        );
        when(agentManagementService.getInstanceGroups()).thenReturn(instanceGroups);

        ApplicationSLA smallSLA = ApplicationSlaSample.CriticalSmall.builder().withInstanceCount(4).build(); // 4 CPUs
        CapacityRequirements requirements = new CapacityRequirements(singletonMap(Tier.Critical, singletonList(smallSLA)));

        // Our SLA allocation requires 1 instance, but buffer goes over it, so we require 2
        CapacityAllocations allocations = strategy.compute(requirements);

        allocations.getInstanceGroups().forEach(instanceGroup -> {
            assertThat(instanceGroup.getTier()).isEqualTo(Tier.Critical);
        });

        AgentInstanceGroup r48xlInstanceGroup = findInstanceGroupByInstanceType(allocations.getInstanceGroups(), M4_XLARGE_ID);
        assertThat(allocations.getExpectedMinSize(r48xlInstanceGroup)).isEqualTo(2);
    }

    @Test
    public void testAllocationNotChangedIfNoAppInTier() {
        List<AgentInstanceGroup> instanceGroups = singletonList(
                getInstanceGroup(Tier.Critical, M4_XLARGE_ID, 0, 1)
        );
        when(agentManagementService.getInstanceGroups()).thenReturn(instanceGroups);

        CapacityRequirements requirements = new CapacityRequirements(Collections.emptyMap());
        CapacityAllocations allocations = strategy.compute(requirements);
        assertThat(allocations.getInstanceGroups()).isEmpty();
    }

    @Test
    public void testAllocationNotChangedIfOnlyDefaultAppInFlexTier() {
        List<AgentInstanceGroup> instanceGroups = singletonList(
                getInstanceGroup(Tier.Flex, R4_8XLARGE_ID, 0, 1)
        );
        when(agentManagementService.getInstanceGroups()).thenReturn(instanceGroups);

        CapacityRequirements requirements = new CapacityRequirements(singletonMap(Tier.Flex, singletonList(ApplicationSlaSample.DefaultFlex.build())));
        CapacityAllocations allocations = strategy.compute(requirements);
        assertThat(allocations.getInstanceGroups()).isEmpty();
    }

    @Test
    public void testAllocationsMinLimitsAreApplied() {
        List<AgentInstanceGroup> instanceGroups = asList(
                getInstanceGroup(Tier.Critical, M4_XLARGE_ID, 2, 10),
                getInstanceGroup(Tier.Critical, M4_4XLARGE_ID, 3, 10),
                getInstanceGroup(Tier.Critical, P2_8XLARGE_ID, 5, 10)
        );
        when(agentManagementService.getInstanceGroups()).thenReturn(instanceGroups);

        ApplicationSLA smallSLA = ApplicationSlaSample.CriticalSmall.builder().withInstanceCount(1).build();

        CapacityRequirements requirements = new CapacityRequirements(singletonMap(Tier.Critical, singletonList(smallSLA)));
        CapacityAllocations allocations = strategy.compute(requirements);

        AgentInstanceGroup m4xlInstanceGroup = findInstanceGroupByInstanceType(allocations.getInstanceGroups(), M4_XLARGE_ID);
        assertThat(allocations.getExpectedMinSize(m4xlInstanceGroup)).isEqualTo(2);

        AgentInstanceGroup m44xlInstanceGroup = findInstanceGroupByInstanceType(allocations.getInstanceGroups(), M4_4XLARGE_ID);
        assertThat(allocations.getExpectedMinSize(m44xlInstanceGroup)).isEqualTo(3);

        AgentInstanceGroup p28xlInstanceGroup = findInstanceGroupByInstanceType(allocations.getInstanceGroups(), P2_8XLARGE_ID);
        assertThat(allocations.getExpectedMinSize(p28xlInstanceGroup)).isEqualTo(5);
    }

    @Test
    public void testResourceShortageReporting() {
        List<AgentInstanceGroup> instanceGroups = singletonList(
                getInstanceGroup(Tier.Critical, M4_XLARGE_ID, 0, 2)
        );
        when(agentManagementService.getInstanceGroups()).thenReturn(instanceGroups);

        ApplicationSLA veryBigSLA = ApplicationSlaSample.CriticalSmall.builder().withInstanceCount(100).build();
        CapacityRequirements requirements = new CapacityRequirements(singletonMap(Tier.Critical, singletonList(veryBigSLA)));

        CapacityAllocations allocations = strategy.compute(requirements);
        assertThat(allocations.getTiersWithResourceShortage()).containsExactly(Tier.Critical);
    }

    private AgentInstanceGroup findInstanceGroupByInstanceType(Collection<AgentInstanceGroup> instanceGroups, String instanceType) {
        return instanceGroups.stream().filter(instanceGroup -> instanceGroup.getInstanceType().equals(instanceType)).findFirst().orElse(null);
    }

    private AgentInstanceGroup getInstanceGroup(Tier tier, String instanceType, int min, int max) {
        AutoScaleRule autoScaleRule = AutoScaleRule.newBuilder()
                .withMin(min)
                .build();
        return AgentGenerator.agentServerGroups(tier, 0, singletonList(instanceType)).getValue()
                .toBuilder()
                .withResourceDimension(ResourceDimension.empty())
                .withMin(0)
                .withMax(max)
                .withAutoScaleRule(autoScaleRule)
                .build();
    }
}