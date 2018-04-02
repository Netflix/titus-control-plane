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

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.master.service.management.BeanCapacityManagementConfiguration;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.aws.AwsInstanceType;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.master.service.management.BeanCapacityManagementConfiguration;
import com.netflix.titus.master.service.management.CapacityGuaranteeStrategy;
import com.netflix.titus.master.service.management.CapacityGuaranteeStrategy.CapacityAllocations;
import com.netflix.titus.master.service.management.CapacityGuaranteeStrategy.CapacityRequirements;
import com.netflix.titus.master.service.management.CapacityManagementConfiguration;
import com.netflix.titus.master.store.ApplicationSlaStore;
import com.netflix.titus.testkit.data.core.ApplicationSlaSample;
import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Test;
import rx.Completable;
import rx.Observable;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultCapacityMonitoringServiceTest {

    private static final ApplicationSLA CRITICAL_APP = ApplicationSlaSample.CriticalSmall.build();
    private static final ApplicationSLA FLEX_APP = ApplicationSlaSample.FlexSmall.build();

    private static final String CRITICAL_INSTANCE_GROUP_ID = "Critical#0";
    private static final String FLEX_INSTANCE_GROUP_ID = "Flex#0";

    private final TestScheduler testScheduler = Schedulers.test();

    private final CapacityManagementConfiguration configuration = BeanCapacityManagementConfiguration.newBuilder()
            .withCriticalTierBuffer(0.1)
            .withFlexTierBuffer(0.1)
            .build();

    private final AgentManagementService agentManagementService = mock(AgentManagementService.class);

    private final ApplicationSlaStore storage = mock(ApplicationSlaStore.class);

    private final CapacityGuaranteeStrategy capacityGuaranteeStrategy = mock(CapacityGuaranteeStrategy.class);

    private final DefaultCapacityMonitoringService capacityMonitoringService = new DefaultCapacityMonitoringService(
            configuration,
            capacityGuaranteeStrategy,
            storage,
            agentManagementService,
            new DefaultRegistry(),
            testScheduler
    );

    @Test
    public void testDirectRefresh() throws Exception {
        setupMocksForApp(1, 1, CRITICAL_APP, FLEX_APP);

        // Refresh
        ExtTestSubscriber<Void> replySubscriber = new ExtTestSubscriber<>();
        capacityMonitoringService.refresh().subscribe(replySubscriber);

        // Check that agentManagementService is triggered correctly
        testScheduler.triggerActions();
        verify(agentManagementService, times(1))
                .updateCapacity(CRITICAL_INSTANCE_GROUP_ID, Optional.of(1), Optional.empty());
        verify(agentManagementService, times(0))
                .updateCapacity(FLEX_INSTANCE_GROUP_ID, Optional.of(1), Optional.empty());

        replySubscriber.assertOnCompleted();
    }

    @Test
    public void testScheduledRefresh() throws Exception {
        setupMocksForApp(1, 1, CRITICAL_APP, FLEX_APP);

        capacityMonitoringService.enterActiveMode();

        testScheduler.triggerActions();
        verify(agentManagementService, times(1))
                .updateCapacity(CRITICAL_INSTANCE_GROUP_ID, Optional.of(1), Optional.empty());
        verify(agentManagementService, times(0))
                .updateCapacity(FLEX_INSTANCE_GROUP_ID, Optional.of(1), Optional.empty());

        // Next refresh round
        AgentInstanceGroup criticalInstanceGroup = getCriticalInstanceGroup(2, 2);
        CapacityAllocations allocations = new CapacityAllocations(singletonMap(criticalInstanceGroup, 2), emptyMap());
        when(capacityGuaranteeStrategy.compute(any())).thenReturn(allocations);

        when(agentManagementService.updateCapacity(any(), any(), any())).thenReturn(Completable.complete());

        testScheduler.advanceTimeBy(DefaultCapacityMonitoringService.PERIODIC_UPDATE_INTERVAL_MS, TimeUnit.MILLISECONDS);
        verify(agentManagementService, times(1))
                .updateCapacity(CRITICAL_INSTANCE_GROUP_ID, Optional.of(2), Optional.empty());
    }

    @Test
    public void testScheduledRefreshRecoversFromError() throws Exception {
        when(storage.findAll()).thenReturn(Observable.error(new IOException("simulated storage error")));

        capacityMonitoringService.enterActiveMode();
        testScheduler.triggerActions();
        verify(agentManagementService, times(0)).updateCapacity(any(), any(), any());

        // Next refresh round
        setupMocksForApp(1, 1, CRITICAL_APP, FLEX_APP);

        testScheduler.advanceTimeBy(DefaultCapacityMonitoringService.PERIODIC_UPDATE_INTERVAL_MS, TimeUnit.MILLISECONDS);
        verify(agentManagementService, times(1))
                .updateCapacity(CRITICAL_INSTANCE_GROUP_ID, Optional.of(1), Optional.empty());
        verify(agentManagementService, times(0))
                .updateCapacity(FLEX_INSTANCE_GROUP_ID, Optional.of(1), Optional.empty());
    }

    @Test
    public void testScheduledRefreshRecoversFromTimeout() throws Exception {
        setupMocksForApp(1, 1, CRITICAL_APP, FLEX_APP);

        reset(agentManagementService);
        when(agentManagementService.updateCapacity(any(), any(), any())).thenReturn(Completable.never());

        capacityMonitoringService.enterActiveMode();
        testScheduler.triggerActions(); // This triggers immediately the periodically scheduled refresh

        // Test we are stuck in not completing update capacity call
        capacityMonitoringService.refresh().subscribe();

        testScheduler.advanceTimeBy(DefaultCapacityMonitoringService.UPDATE_TIMEOUT_MS - 1, TimeUnit.MILLISECONDS);

        // Force timeout to trigger
        when(agentManagementService.updateCapacity(any(), any(), any())).thenReturn(Completable.complete());

        testScheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);

        // Verify that the first time that blocked and the second time occurred
        verify(agentManagementService, times(2))
                .updateCapacity(CRITICAL_INSTANCE_GROUP_ID, Optional.of(1), Optional.empty());
        verify(agentManagementService, times(0))
                .updateCapacity(FLEX_INSTANCE_GROUP_ID, Optional.of(1), Optional.empty());
    }

    private void setupMocksForApp(int criticalInstances, int flexInstances, ApplicationSLA... apps) {
        when(storage.findAll()).thenReturn(Observable.from(apps));

        AgentInstanceGroup criticalInstanceGroup = getCriticalInstanceGroup(criticalInstances, 1);
        AgentInstanceGroup flexInstanceGroup = getFlexInstanceGroup(flexInstances, 1);

        CapacityAllocations allAllocations = new CapacityAllocations(CollectionsExt.<AgentInstanceGroup, Integer>newHashMap()
                .entry(criticalInstanceGroup, criticalInstances)
                .entry(flexInstanceGroup, flexInstances)
                .toMap()
                , emptyMap());

        CapacityAllocations scalableAllocations = new CapacityAllocations(CollectionsExt.<AgentInstanceGroup, Integer>newHashMap()
                .entry(criticalInstanceGroup, criticalInstances)
                .toMap()
                , emptyMap());

        when(capacityGuaranteeStrategy.compute(any())).thenAnswer(i -> {
            if (i.getArguments()[0] == null) { // mock setup
                return null;
            }
            CapacityRequirements req = (CapacityRequirements) i.getArguments()[0];
            return req.getTiers().contains(Tier.Flex) ? allAllocations : scalableAllocations;
        });

        when(agentManagementService.updateCapacity(any(), any(), any())).thenReturn(Completable.complete());
    }

    private AgentInstanceGroup getCriticalInstanceGroup(int min, int max) {
        return AgentInstanceGroup.newBuilder()
                .withId(CRITICAL_INSTANCE_GROUP_ID)
                .withTier(Tier.Critical)
                .withInstanceType(AwsInstanceType.M4_4XLARGE_ID)
                .withMin(min)
                .withMax(max)
                .build();
    }

    private AgentInstanceGroup getFlexInstanceGroup(int min, int max) {
        return AgentInstanceGroup.newBuilder()
                .withId(FLEX_INSTANCE_GROUP_ID)
                .withTier(Tier.Flex)
                .withInstanceType(AwsInstanceType.R4_8XLARGE_ID)
                .withMin(min)
                .withMax(max)
                .build();
    }
}