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

import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import com.netflix.spectator.api.DefaultRegistry;
import io.netflix.titus.api.model.ResourceDimension;
import io.netflix.titus.api.model.Tier;
import io.netflix.titus.common.aws.AwsInstanceDescriptor;
import io.netflix.titus.common.aws.AwsInstanceType;
import io.netflix.titus.master.agent.service.server.ServerInfoResolver;
import io.netflix.titus.master.agent.service.server.ServerInfoResolvers;
import io.netflix.titus.master.service.management.CapacityAllocationService;
import io.netflix.titus.master.service.management.CapacityManagementConfiguration;
import io.netflix.titus.master.service.management.CapacityManagementConfiguration.TierConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

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

    private final CapacityAllocationService capacityAllocationService = mock(CapacityAllocationService.class);

    private final ServerInfoResolver serverInfoResolver = mock(ServerInfoResolver.class);

    private final DefaultAvailableCapacityService availableCapacityService = new DefaultAvailableCapacityService(
            configuration,
            capacityAllocationService,
            serverInfoResolver,
            new DefaultRegistry(),
            testScheduler
    );

    @Before
    public void setUp() throws Exception {
        TierConfig critical = mock(TierConfig.class);
        when(critical.getInstanceTypes()).thenReturn(new String[]{AwsInstanceType.M4_XLARGE_ID});

        TierConfig flex = mock(TierConfig.class);
        when(flex.getInstanceTypes()).thenReturn(new String[]{AwsInstanceType.R3_4XLARGE_ID});

        when(configuration.getTiers()).thenReturn(
                ImmutableMap.<String, TierConfig>builder().put("0", critical).put("1", flex).build()
        );
        when(configuration.getAvailableCapacityUpdateIntervalMs()).thenReturn(30000L);

        when(capacityAllocationService.limits(singletonList(AwsInstanceType.M4_XLARGE_ID))).thenReturn(
                Observable.just(singletonList(CRITICAL_INSTANCE_COUNT))
        );
        when(capacityAllocationService.limits(singletonList(AwsInstanceType.R3_4XLARGE_ID))).thenReturn(
                Observable.just(singletonList(FLEX_INSTANCE_COUNT))
        );

        ServerInfoResolver resolver = ServerInfoResolvers.fromAwsInstanceTypes();
        when(this.serverInfoResolver.resolve(AwsInstanceType.M4_XLARGE_ID)).thenReturn(
                resolver.resolve(AwsInstanceType.M4_XLARGE_ID)
        );
        when(this.serverInfoResolver.resolve(AwsInstanceType.R3_4XLARGE_ID)).thenReturn(
                resolver.resolve(AwsInstanceType.R3_4XLARGE_ID)
        );

        availableCapacityService.enterActiveMode();
    }

    @After
    public void tearDown() throws Exception {
        availableCapacityService.shutdown();
    }

    @Test
    public void testCapacityComputationCorrectness() throws Exception {
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