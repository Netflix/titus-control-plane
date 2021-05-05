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

package com.netflix.titus.master.scheduler;

import java.util.Optional;

import com.netflix.fenzo.queues.tiered.TieredQueueSlas;
import com.netflix.fenzo.sla.ResAllocs;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.master.model.ResourceDimensions;
import com.netflix.titus.master.scheduler.DefaultTierSlaUpdater.MyTieredQueueSlas;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import com.netflix.titus.master.service.management.AvailableCapacityService;
import com.netflix.titus.testkit.data.core.ApplicationSlaSample;
import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Before;
import org.junit.Test;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static com.netflix.titus.api.model.SchedulerConstants.SCHEDULER_NAME_FENZO;
import static com.netflix.titus.master.model.ResourceDimensions.fromResAllocs;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultTierSlaUpdaterTest {

    private final TestScheduler testScheduler = Schedulers.test();

    private static final ResourceDimension CRITICAL_CAPACITY = ResourceDimension.newBuilder()
            .withCpus(10)
            .withMemoryMB(64 * 1024)
            .withDiskMB(100 * 1024)
            .withNetworkMbs(10 * 1000)
            .build();
    private static final ResourceDimension FLEX_CAPACITY = ResourceDimensions.multiply(CRITICAL_CAPACITY, 2);

    private static final ApplicationSLA CRITICAL_CAPACITY_GROUP = ApplicationSlaSample.CriticalLarge.build();
    private static final ApplicationSLA FLEX_CAPACITY_GROUP = ApplicationSlaSample.FlexLarge.build();

    private final SchedulerConfiguration config = mock(SchedulerConfiguration.class);

    private final ApplicationSlaManagementService applicationSlaManagementService = mock(ApplicationSlaManagementService.class);

    private final AvailableCapacityService availableCapacityService = mock(AvailableCapacityService.class);

    private final DefaultTierSlaUpdater updater = new DefaultTierSlaUpdater(config, applicationSlaManagementService, availableCapacityService, testScheduler);

    private final ExtTestSubscriber<TieredQueueSlas> testSubscriber = new ExtTestSubscriber<>();

    @Before
    public void setUp() throws Exception {
        when(config.getTierSlaUpdateIntervalMs()).thenReturn(30000L);
        when(availableCapacityService.totalCapacityOf(Tier.Critical)).thenReturn(Optional.of(CRITICAL_CAPACITY));
        when(availableCapacityService.totalCapacityOf(Tier.Flex)).thenReturn(Optional.of(FLEX_CAPACITY));

        when(applicationSlaManagementService.getApplicationSLAsForScheduler(SCHEDULER_NAME_FENZO)).thenReturn(
                asList(FLEX_CAPACITY_GROUP, CRITICAL_CAPACITY_GROUP)
        );
    }

    @Test
    public void testTierSlaComputationCorrectness() throws Exception {
        updater.tieredQueueSlaUpdates().subscribe(testSubscriber);

        testScheduler.triggerActions();
        MyTieredQueueSlas slas = (MyTieredQueueSlas) testSubscriber.takeNext();

        // Verify critical tier
        ResAllocs criticalTierCapacity = slas.getTierCapacities().get(Tier.Critical.ordinal());
        assertThat(fromResAllocs(criticalTierCapacity)).isEqualTo(CRITICAL_CAPACITY);

        // Verify critical capacity group
        ResAllocs criticalGrCapActual = slas.getGroupCapacities().get(Tier.Critical.ordinal()).get(CRITICAL_CAPACITY_GROUP.getAppName());
        assertThat(fromResAllocs(criticalGrCapActual)).isEqualTo(toResourceDimension(CRITICAL_CAPACITY_GROUP));

        // Verify flex tier
        ResAllocs flexTierCapacity = slas.getTierCapacities().get(Tier.Flex.ordinal());
        assertThat(fromResAllocs(flexTierCapacity)).isEqualTo(FLEX_CAPACITY);

        // Verify flex capacity group
        ResAllocs flexGrCapActual = slas.getGroupCapacities().get(Tier.Flex.ordinal()).get(FLEX_CAPACITY_GROUP.getAppName());
        assertThat(fromResAllocs(flexGrCapActual)).isEqualTo(toResourceDimension(FLEX_CAPACITY_GROUP));
    }

    private static ResourceDimension toResourceDimension(ApplicationSLA applicationSLA) {
        return ResourceDimensions.fromResAllocs(DefaultTierSlaUpdater.toResAllocs(applicationSLA));
    }
}