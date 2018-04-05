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

package com.netflix.titus.master.agent.service.monitor;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.monitor.AgentStatus;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.testkit.model.agent.AgentGenerator;
import org.junit.Before;
import org.junit.Test;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static com.netflix.titus.testkit.model.agent.AgentGenerator.agentInstances;
import static java.util.Collections.singletonList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PollingStatusMonitorTest {

    private static final String MY_SOURCE_ID = "TEST";

    private final TestScheduler testScheduler = Schedulers.test();

    private final MonitorTestSubscriber monitorSubscriber = new MonitorTestSubscriber();

    private final AgentManagementService agentManagementService = mock(AgentManagementService.class);

    private AgentInstanceGroup instanceGroup;
    private AgentInstance agentInstance;

    private AgentStatus statusOk;
    private AgentStatus statusBad;

    private AgentStatus currentAgentStatus;

    @Before
    public void setUp() throws Exception {
        this.instanceGroup = AgentGenerator.agentServerGroups().getValue();
        this.agentInstance = agentInstances(instanceGroup).getValue();
        this.statusOk = AgentStatus.healthy(MY_SOURCE_ID, agentInstance, "Ok", 0);
        this.statusBad = AgentStatus.unhealthy(MY_SOURCE_ID, agentInstance, "Bad", 0);

        this.currentAgentStatus = statusOk;

        when(agentManagementService.findAgentInstances(any())).thenReturn(singletonList(Pair.of(instanceGroup, singletonList(agentInstance))));

        new MyPollingStatusMonitor().monitor().subscribe(monitorSubscriber);
    }

    @Test
    public void testHealthStatusIsCheckedPeriodically() throws Exception {
        // Immediate check at the subscription time
        testScheduler.triggerActions();
        verify(agentManagementService, times(1)).findAgentInstances(any());
        monitorSubscriber.verifyEmitted(statusOk);

        // Next healthy check
        testScheduler.advanceTimeBy(DefaultConfiguration.CONFIG.getHealthPollingInterval(), TimeUnit.MILLISECONDS);
        verify(agentManagementService, times(2)).findAgentInstances(any());
        monitorSubscriber.verifyNothingEmitted();

        // Next unhealthy check
        currentAgentStatus = statusBad;
        testScheduler.advanceTimeBy(DefaultConfiguration.CONFIG.getHealthPollingInterval(), TimeUnit.MILLISECONDS);
        verify(agentManagementService, times(3)).findAgentInstances(any());
        monitorSubscriber.verifyEmitted(statusBad);
    }

    @Test
    public void testExceptionInHealthCheckResolverDoesNotBreakMonitor() throws Exception {
        // First healthcheck is failing
        currentAgentStatus = null;
        testScheduler.triggerActions();
        verify(agentManagementService, times(1)).findAgentInstances(any());
        monitorSubscriber.assertNoValues();
        monitorSubscriber.assertNoTerminalEvent();

        // Next healthcheck is healthy
        currentAgentStatus = statusOk;
        testScheduler.advanceTimeBy(DefaultConfiguration.CONFIG.getHealthPollingInterval(), TimeUnit.MILLISECONDS);
        verify(agentManagementService, times(2)).findAgentInstances(any());
        monitorSubscriber.verifyEmitted(statusOk);
    }

    @Test
    public void testExceptionInAgentManagementServiceDoesNotBreakMonitor() throws Exception {
        when(agentManagementService.findAgentInstances(any()))
                .thenThrow(new RuntimeException("simulated agent resolver error"))
                .thenReturn(singletonList(Pair.of(instanceGroup, singletonList(agentInstance))));

        // First agent resolve is failing
        testScheduler.triggerActions();
        verify(agentManagementService, times(1)).findAgentInstances(any());
        monitorSubscriber.assertNoValues();
        monitorSubscriber.assertNoTerminalEvent();

        // Next agent resolve is healthy
        testScheduler.advanceTimeBy(DefaultConfiguration.CONFIG.getHealthPollingInterval(), TimeUnit.MILLISECONDS);
        verify(agentManagementService, times(2)).findAgentInstances(any());
        monitorSubscriber.verifyEmitted(statusOk);
    }

    private class MyPollingStatusMonitor extends PollingStatusMonitor {

        MyPollingStatusMonitor() {
            super(MY_SOURCE_ID, agentManagementService, DefaultConfiguration.CONFIG, new DefaultRegistry(), testScheduler);
        }

        @Override
        protected AgentStatus resolve(AgentInstance agentInstance) {
            Preconditions.checkNotNull(currentAgentStatus, "Agent status not available");
            return currentAgentStatus;
        }
    }
}