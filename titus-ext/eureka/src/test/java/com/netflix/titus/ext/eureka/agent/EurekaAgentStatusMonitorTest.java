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

package com.netflix.titus.ext.eureka.agent;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.discovery.CacheRefreshedEvent;
import com.netflix.discovery.EurekaClient;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.monitor.AgentStatus;
import com.netflix.titus.api.agent.model.monitor.AgentStatus.AgentStatusCode;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.aws.AwsInstanceType;
import com.netflix.titus.testkit.model.agent.AgentDeployment;
import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Before;
import org.junit.Test;

import static com.netflix.titus.testkit.model.agent.AgentDeployment.instrumentMock;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

public class EurekaAgentStatusMonitorTest {

    private final EurekaClient eurekaClient = mock(EurekaClient.class);

    private final AgentDeployment agentDeployment = AgentDeployment.newDeployment()
            .withActiveInstanceGroup(Tier.Flex, "f1", AwsInstanceType.M4_4XLarge, 1)
            .build();

    private final AgentInstance instance = agentDeployment.getFirstInstance();

    private final AgentManagementService agentManagementService = instrumentMock(agentDeployment, mock(AgentManagementService.class));

    private final EurekaAgentStatusMonitor monitor = new EurekaAgentStatusMonitor(eurekaClient, agentManagementService, new DefaultRegistry());

    @Before
    public void setUp() throws Exception {
        monitor.enterActiveMode();
    }

    @Test
    public void testInstanceUpInEurekaIsHealthy() throws Exception {
        mockStatusInEureka(instance, InstanceStatus.UP);
        AgentStatus status = monitor.getStatus(instance.getId());
        assertThat(status.getStatusCode()).isEqualTo(AgentStatusCode.Healthy);
    }

    @Test
    public void testInstanceNotUpInEurekaIsUnhealthy() throws Exception {
        for (InstanceStatus status : asList(InstanceStatus.DOWN, InstanceStatus.STARTING, InstanceStatus.OUT_OF_SERVICE, InstanceStatus.UNKNOWN)) {
            mockStatusInEureka(instance, status);
            AgentStatus agentStatus = monitor.getStatus(instance.getId());
            assertThat(agentStatus.getStatusCode()).isEqualTo(AgentStatusCode.Unhealthy);
        }
    }

    @Test
    public void testInstanceNotRegisteredWithEurekaIsUnhealthy() throws Exception {
        AgentStatus status = monitor.getStatus(instance.getId());
        assertThat(status.getStatusCode()).isEqualTo(AgentStatusCode.Unhealthy);
    }

    @Test
    public void testTerminatedInstanceEvent() throws Exception {
        ExtTestSubscriber<AgentStatus> testSubscriber = new ExtTestSubscriber<>();
        monitor.monitor().subscribe(testSubscriber);

        // Trigger instance up event.
        mockStatusInEureka(instance, InstanceStatus.UP);
        assertThat(testSubscriber.takeNext().getStatusCode()).isEqualTo(AgentStatusCode.Healthy);

        // Simulate termination of an agent instance.
        reset(agentManagementService);
        monitor.onEvent(new CacheRefreshedEvent());
        assertThat(testSubscriber.takeNext().getStatusCode()).isEqualTo(AgentStatusCode.Terminated);
    }

    @Test
    public void testEurekaRegistrationChangesTriggerStatusUpdateInEventStream() throws Exception {
        ExtTestSubscriber<AgentStatus> testSubscriber = new ExtTestSubscriber<>();
        monitor.monitor().subscribe(testSubscriber);

        // UP
        mockStatusInEureka(instance, InstanceStatus.UP);
        assertThat(testSubscriber.takeNext().getStatusCode()).isEqualTo(AgentStatusCode.Healthy);

        // DOWN
        mockStatusInEureka(instance, InstanceStatus.DOWN);
        assertThat(testSubscriber.takeNext().getStatusCode()).isEqualTo(AgentStatusCode.Unhealthy);

        // Back to UP
        mockStatusInEureka(instance, InstanceStatus.UP);
        assertThat(testSubscriber.takeNext().getStatusCode()).isEqualTo(AgentStatusCode.Healthy);
    }

    private void mockStatusInEureka(AgentInstance instance, InstanceStatus eurekaStatus) {
        InstanceInfo instanceInfo = InstanceInfo.Builder.newBuilder()
                .setInstanceId(instance.getId())
                .setAppName("testApp")
                .setStatus(eurekaStatus)
                .build();
        when(eurekaClient.getInstancesById(instance.getId())).thenReturn(singletonList(instanceInfo));

        monitor.onEvent(new CacheRefreshedEvent());
    }
}
