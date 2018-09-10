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

package com.netflix.titus.master.service.management.internal;

import java.util.Optional;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.aws.AwsInstanceDescriptor;
import com.netflix.titus.common.aws.AwsInstanceType;
import com.netflix.titus.master.agent.service.server.ServerInfoResolver;
import com.netflix.titus.master.agent.service.server.ServerInfoResolvers;
import com.netflix.titus.master.service.management.CapacityManagementConfiguration;
import com.netflix.titus.testkit.model.agent.AgentGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static com.netflix.titus.common.aws.AwsInstanceType.M4_XLARGE_ID;
import static com.netflix.titus.common.aws.AwsInstanceType.R4_8XLARGE_ID;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class DefaultAvailableCapacityServiceTest {

    private static final int CRITICAL_INSTANCE_COUNT = 2;
    private static final int FLEX_INSTANCE_COUNT = 4;

    private final TestScheduler testScheduler = Schedulers.test();

    private final CapacityManagementConfiguration configuration = mock(CapacityManagementConfiguration.class);

    private final AgentManagementService agentManagementService = mock(AgentManagementService.class);

    private final ServerInfoResolver serverInfoResolver = mock(ServerInfoResolver.class);

    private final DefaultAvailableCapacityService availableCapacityService = new DefaultAvailableCapacityService(
            configuration,
            serverInfoResolver,
            agentManagementService,
            new DefaultRegistry(),
            testScheduler
    );

    @Before
    public void setUp() throws Exception {
        when(configuration.getAvailableCapacityUpdateIntervalMs()).thenReturn(30000L);

        ServerInfoResolver resolver = ServerInfoResolvers.fromAwsInstanceTypes();
        when(this.serverInfoResolver.resolve(M4_XLARGE_ID)).thenReturn(
                resolver.resolve(M4_XLARGE_ID)
        );
        when(this.serverInfoResolver.resolve(R4_8XLARGE_ID)).thenReturn(
                resolver.resolve(R4_8XLARGE_ID)
        );

        AgentInstanceGroup criticalInstanceGroup = AgentGenerator.agentServerGroups(Tier.Critical, 0, singletonList(M4_XLARGE_ID)).getValue()
                .toBuilder()
                .withCurrent(CRITICAL_INSTANCE_COUNT)
                .withMax(CRITICAL_INSTANCE_COUNT)
                .build();

        AgentInstanceGroup flexInstanceGroup = AgentGenerator.agentServerGroups(Tier.Flex, 0, singletonList(R4_8XLARGE_ID)).getValue()
                .toBuilder()
                .withMax(FLEX_INSTANCE_COUNT)
                .build();

        when(agentManagementService.getInstanceGroups()).thenReturn(asList(criticalInstanceGroup, flexInstanceGroup));

        availableCapacityService.enterActiveMode();
    }

    @After
    public void tearDown() {
        availableCapacityService.shutdown();
    }

    @Test
    public void testCapacityComputationCorrectness() {
        testScheduler.triggerActions();

        Optional<ResourceDimension> capacity = availableCapacityService.totalCapacityOf(Tier.Critical);
        assertThat(capacity).isPresent();

        ResourceDimension criticalExpected = dimensionOf(AwsInstanceType.M4_XLarge, CRITICAL_INSTANCE_COUNT);
        assertThat(capacity).contains(criticalExpected);
    }

    private ResourceDimension dimensionOf(AwsInstanceType instanceType, int count) {
        AwsInstanceDescriptor descriptor = instanceType.getDescriptor();
        return ResourceDimension.newBuilder()
                .withCpus(descriptor.getvCPUs() * count)
                .withMemoryMB(descriptor.getMemoryGB() * 1024 * count)
                .withDiskMB(descriptor.getStorageGB() * 1024 * count)
                .withNetworkMbs(descriptor.getNetworkMbs() * count)
                .build();
    }
}