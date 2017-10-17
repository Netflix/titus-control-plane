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

package io.netflix.titus.master.agent.service.monitor;

import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.DefaultRegistry;
import io.netflix.titus.api.agent.model.AgentInstance;
import io.netflix.titus.api.agent.model.AgentInstanceGroup;
import io.netflix.titus.api.agent.model.monitor.AgentStatus;
import io.netflix.titus.api.agent.service.AgentManagementService;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.testkit.model.agent.AgentGenerator;
import org.junit.Before;
import org.junit.Test;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static io.netflix.titus.testkit.model.agent.AgentGenerator.agentInstances;
import static java.util.Collections.singletonList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HealthStatusMonitorTest {

    private final TestScheduler testScheduler = Schedulers.test();
    private final MonitorTestSubscriber monitorSubscriber = new MonitorTestSubscriber();

    private final AgentManagementService agentManagementService = mock(AgentManagementService.class);
    private final HealthStatusMonitor.AgentHealthResolver healthResolver = mock(HealthStatusMonitor.AgentHealthResolver.class);

    private AgentInstanceGroup instanceGroup;
    private AgentInstance agentInstance;
    private AgentStatus statusOk;

    @Before
    public void setUp() throws Exception {
        this.instanceGroup = AgentGenerator.agentServerGroups().getValue();
        this.agentInstance = agentInstances(instanceGroup).getValue();
        this.statusOk = AgentStatus.healthy(HealthStatusMonitor.SOURCE_ID, agentInstance);

        when(agentManagementService.findAgentInstances(any())).thenReturn(singletonList(Pair.of(instanceGroup, singletonList(agentInstance))));
        when(healthResolver.isHealthy(statusOk.getInstance().getId())).thenReturn(true);

        new HealthStatusMonitor(agentManagementService, healthResolver, DefaultConfiguration.CONFIG, new DefaultRegistry(), testScheduler)
                .monitor()
                .subscribe(monitorSubscriber);
    }

    @Test
    public void testHealthStatusIsCheckedPeriodically() throws Exception {
        // Immediate check at the subscription time
        testScheduler.triggerActions();
        verify(agentManagementService, times(1)).findAgentInstances(any());
        verify(healthResolver, times(1)).isHealthy(statusOk.getInstance().getId());
        monitorSubscriber.verifyEmitted(statusOk);

        // Next healthy check
        testScheduler.advanceTimeBy(DefaultConfiguration.CONFIG.getHealthPollingInterval(), TimeUnit.MILLISECONDS);
        verify(agentManagementService, times(2)).findAgentInstances(any());
        verify(healthResolver, times(2)).isHealthy(statusOk.getInstance().getId());
        monitorSubscriber.verifyEmitted(statusOk);

        // Next unhealthy check
        when(healthResolver.isHealthy(statusOk.getInstance().getId())).thenReturn(false);

        testScheduler.advanceTimeBy(DefaultConfiguration.CONFIG.getHealthPollingInterval(), TimeUnit.MILLISECONDS);
        verify(agentManagementService, times(3)).findAgentInstances(any());
        verify(healthResolver, times(3)).isHealthy(statusOk.getInstance().getId());
        monitorSubscriber.verifyEmitted(badFor(agentInstance));
    }

    @Test
    public void testExceptionInHealthCheckResolverDoesNotBreakMonitor() throws Exception {
        when(healthResolver.isHealthy(statusOk.getInstance().getId()))
                .thenThrow(new RuntimeException("simulated healthcheck error"))
                .thenReturn(true);

        // First healthcheck is failing
        testScheduler.triggerActions();
        verify(agentManagementService, times(1)).findAgentInstances(any());
        verify(healthResolver, times(1)).isHealthy(statusOk.getInstance().getId());
        monitorSubscriber.assertNoValues();
        monitorSubscriber.assertNoTerminalEvent();

        // Next healthcheck is healthy
        testScheduler.advanceTimeBy(DefaultConfiguration.CONFIG.getHealthPollingInterval(), TimeUnit.MILLISECONDS);
        verify(agentManagementService, times(2)).findAgentInstances(any());
        verify(healthResolver, times(2)).isHealthy(statusOk.getInstance().getId());
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
        verify(healthResolver, times(0)).isHealthy(statusOk.getInstance().getId());
        monitorSubscriber.assertNoValues();
        monitorSubscriber.assertNoTerminalEvent();

        // Next agent resolve is healthy
        testScheduler.advanceTimeBy(DefaultConfiguration.CONFIG.getHealthPollingInterval(), TimeUnit.MILLISECONDS);
        verify(agentManagementService, times(2)).findAgentInstances(any());
        verify(healthResolver, times(1)).isHealthy(statusOk.getInstance().getId());
        monitorSubscriber.verifyEmitted(statusOk);
    }

    private AgentStatus badFor(AgentInstance agentInstance) {
        return AgentStatus.unhealthy(HealthStatusMonitor.SOURCE_ID, agentInstance, HealthStatusMonitor.UNHEALTHY_NODE_DISABLE_TIME, testScheduler.now());
    }
}