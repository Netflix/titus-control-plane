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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.DefaultRegistry;
import io.netflix.titus.api.model.ApplicationSLA;
import io.netflix.titus.api.model.Tier;
import io.netflix.titus.common.aws.AwsInstanceType;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.master.service.management.BeanCapacityManagementConfiguration;
import io.netflix.titus.master.service.management.BeanTierConfig;
import io.netflix.titus.master.service.management.CapacityAllocationService;
import io.netflix.titus.master.service.management.CapacityGuaranteeStrategy;
import io.netflix.titus.master.service.management.CapacityGuaranteeStrategy.CapacityAllocations;
import io.netflix.titus.master.service.management.CapacityGuaranteeStrategy.CapacityRequirements;
import io.netflix.titus.master.service.management.CapacityManagementConfiguration;
import io.netflix.titus.master.store.ApplicationSlaStore;
import io.netflix.titus.testkit.data.core.ApplicationSlaSample;
import io.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Test;
import rx.Observable;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultCapacityMonitoringServiceTest {

    private static final ApplicationSLA CRITICAL_APP = ApplicationSlaSample.CriticalSmall.build();
    private static final ApplicationSLA FLEX_APP = ApplicationSlaSample.FlexSmall.build();

    private static final String CRITICAL_INSTANCE = AwsInstanceType.M4_4XLARGE_ID;
    private static final String FLEX_INSTANCE = AwsInstanceType.R3_8XLARGE_ID;

    private final TestScheduler testScheduler = Schedulers.test();

    private final CapacityManagementConfiguration configuration = BeanCapacityManagementConfiguration.newBuilder()
            .withCriticalTier(BeanTierConfig.newBuilder().withBuffer(0.1).withInstanceTypes(CRITICAL_INSTANCE))
            .withFlexTier(BeanTierConfig.newBuilder().withBuffer(0.1).withInstanceTypes(FLEX_INSTANCE))
            .build();

    private final CapacityAllocationService capacityAllocationService = mock(CapacityAllocationService.class);

    private final ApplicationSlaStore storage = mock(ApplicationSlaStore.class);

    private final CapacityGuaranteeStrategy capacityGuaranteeStrategy = mock(CapacityGuaranteeStrategy.class);

    private final DefaultCapacityMonitoringService capacityMonitoringService = new DefaultCapacityMonitoringService(
            capacityAllocationService,
            configuration,
            capacityGuaranteeStrategy,
            storage,
            new DefaultRegistry(),
            testScheduler
    );

    @Test
    public void testDirectRefresh() throws Exception {
        setupMocksForApp(1, 1, CRITICAL_APP, FLEX_APP);

        // Refresh
        ExtTestSubscriber<Void> replySubscriber = new ExtTestSubscriber<>();
        capacityMonitoringService.refresh().subscribe(replySubscriber);

        // Check that capacityAllocationService is triggered correctly
        testScheduler.triggerActions();
        verify(capacityAllocationService, times(1)).allocate(CRITICAL_INSTANCE, 1);
        verify(capacityAllocationService, times(0)).allocate(FLEX_INSTANCE, 1);

        replySubscriber.assertOnCompleted();
    }

    @Test
    public void testScheduledRefresh() throws Exception {
        setupMocksForApp(1, 1, CRITICAL_APP, FLEX_APP);

        capacityMonitoringService.enterActiveMode();

        testScheduler.triggerActions();
        verify(capacityAllocationService, times(1)).allocate(CRITICAL_INSTANCE, 1);
        verify(capacityAllocationService, times(0)).allocate(FLEX_INSTANCE, 1);

        // Next refresh round
        CapacityAllocations allocations = new CapacityAllocations(singletonMap(CRITICAL_INSTANCE, 2), emptyMap());
        when(capacityGuaranteeStrategy.compute(any())).thenReturn(allocations);

        when(capacityAllocationService.allocate(CRITICAL_INSTANCE, 2)).thenReturn(Observable.empty());

        testScheduler.advanceTimeBy(DefaultCapacityMonitoringService.PERIODIC_UPDATE_INTERVAL_MS, TimeUnit.MILLISECONDS);
        verify(capacityAllocationService, times(1)).allocate(CRITICAL_INSTANCE, 2);
    }

    @Test
    public void testScheduledRefreshRecoversFromError() throws Exception {
        when(capacityAllocationService.limits(any())).thenReturn(Observable.empty());
        when(storage.findAll()).thenReturn(Observable.error(new IOException("simulated storage error")));

        capacityMonitoringService.enterActiveMode();
        testScheduler.triggerActions();
        verify(capacityAllocationService, times(0)).allocate(anyString(), anyInt());

        // Next refresh round
        setupMocksForApp(1, 1, CRITICAL_APP, FLEX_APP);

        testScheduler.advanceTimeBy(DefaultCapacityMonitoringService.PERIODIC_UPDATE_INTERVAL_MS, TimeUnit.MILLISECONDS);
        verify(capacityAllocationService, times(1)).allocate(CRITICAL_INSTANCE, 1);
        verify(capacityAllocationService, times(0)).allocate(FLEX_INSTANCE, 1);
    }

    @Test
    public void testScheduledRefreshRecoversFromTimeout() throws Exception {
        setupMocksForApp(1, 1, CRITICAL_APP, FLEX_APP);

        reset(capacityAllocationService);
        when(capacityAllocationService.limits(any())).thenReturn(Observable.never());
        when(capacityAllocationService.allocate(any(), anyInt())).thenReturn(Observable.never());

        capacityMonitoringService.enterActiveMode();
        testScheduler.triggerActions(); // This triggers immediately the periodically scheduled refresh

        // Test we are stuck in not completing limits call
        capacityMonitoringService.refresh().subscribe();

        testScheduler.advanceTimeBy(DefaultCapacityMonitoringService.UPDATE_TIMEOUT_MS - 1, TimeUnit.MILLISECONDS);
        verify(capacityAllocationService, times(1)).limits(any());
        verify(capacityAllocationService, times(0)).allocate(any(), anyInt());

        // Now fix the limits, and force timeout to trigger
        when(capacityAllocationService.limits(any())).thenReturn(Observable.just(asList(1, 1)));
        when(capacityAllocationService.allocate(any(), anyInt())).thenReturn(Observable.empty());

        testScheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        verify(capacityAllocationService, times(1)).allocate(CRITICAL_INSTANCE, 1);
        verify(capacityAllocationService, times(0)).allocate(FLEX_INSTANCE, 1);
    }

    private void setupMocksForApp(int criticalInstances, int flexInstances, ApplicationSLA... apps) {
        when(storage.findAll()).thenReturn(Observable.from(apps));

        CapacityAllocations allAllocations = new CapacityAllocations(CollectionsExt.<String, Integer>newHashMap()
                .entry(CRITICAL_INSTANCE, criticalInstances)
                .entry(FLEX_INSTANCE, flexInstances)
                .toMap()
                , emptyMap());
        CapacityAllocations scalableAllocations = new CapacityAllocations(CollectionsExt.<String, Integer>newHashMap()
                .entry(CRITICAL_INSTANCE, criticalInstances)
                .toMap()
                , emptyMap());
        when(capacityGuaranteeStrategy.compute(any())).thenAnswer(i -> {
            if (i.getArguments()[0] == null) { // mock setup
                return null;
            }
            CapacityRequirements req = (CapacityRequirements) i.getArguments()[0];
            return req.getTiers().contains(Tier.Flex) ? allAllocations : scalableAllocations;
        });

        when(capacityAllocationService.limits(any())).thenReturn(Observable.just(asList(1, 1)));
        when(capacityAllocationService.allocate(CRITICAL_INSTANCE, criticalInstances)).thenReturn(Observable.empty());
        when(capacityAllocationService.allocate(CRITICAL_INSTANCE, flexInstances)).thenReturn(Observable.empty());
    }
}