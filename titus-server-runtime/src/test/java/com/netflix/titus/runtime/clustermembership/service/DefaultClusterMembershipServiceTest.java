/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.runtime.clustermembership.service;

import java.time.Duration;

import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadershipState;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipChangeEvent;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipEvent;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipSnapshotEvent;
import com.netflix.titus.api.clustermembership.model.event.LeaderElectionChangeEvent;
import com.netflix.titus.api.health.HealthIndicator;
import com.netflix.titus.api.health.HealthState;
import com.netflix.titus.api.health.HealthStatus;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.testkit.rx.TitusRxSubscriber;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultClusterMembershipServiceTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private static final boolean ACTIVE = true;
    private static final boolean INACTIVE = false;

    private static final HealthStatus HEALTHY = HealthStatus.newBuilder().withHealthState(HealthState.Healthy).build();
    private static final HealthStatus UNHEALTHY = HealthStatus.newBuilder().withHealthState(HealthState.Unhealthy).build();

    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    private final ClusterMembershipServiceConfiguration configuration = mock(ClusterMembershipServiceConfiguration.class);

    private final HealthIndicator healthIndicator = mock(HealthIndicator.class);

    private ClusterMembershipConnectorStub connector = new ClusterMembershipConnectorStub();

    private final TitusRxSubscriber<ClusterMembershipEvent> eventSubscriber = new TitusRxSubscriber<>();

    private DefaultClusterMembershipService service;

    @Before
    public void setUp() {
        when(configuration.getHealthCheckEvaluationIntervalMs()).thenReturn(1L);
        when(configuration.getHealthCheckEvaluationTimeoutMs()).thenReturn(1_000L);
        when(configuration.isLeaderElectionEnabled()).thenReturn(true);

        when(healthIndicator.health()).thenReturn(HEALTHY);
        service = new DefaultClusterMembershipService(configuration, connector, healthIndicator, titusRuntime);

        service.events().subscribe(eventSubscriber);
        assertThat(eventSubscriber.takeNext()).isInstanceOf(ClusterMembershipSnapshotEvent.class);
    }

    @After
    public void tearDown() {
        if (service != null) {
            service.shutdown();
        }
    }

    @Test(timeout = 30_000)
    public void testLeaderElectionWithHealthcheck() throws InterruptedException {
        // Starts Disabled but healthy, so leadership state should change to NoLeader.
        expectLocalLeadershipTransition(ClusterMemberLeadershipState.NonLeader, ACTIVE);

        // Now make it unhealthy
        when(healthIndicator.health()).thenReturn(UNHEALTHY);
        expectLocalLeadershipTransition(ClusterMemberLeadershipState.Disabled, INACTIVE);

        // Now make healthy again
        when(healthIndicator.health()).thenReturn(HEALTHY);
        expectLocalLeadershipTransition(ClusterMemberLeadershipState.NonLeader, ACTIVE);

        // Become leader
        connector.becomeLeader();
        expectLocalLeadershipTransition(ClusterMemberLeadershipState.Leader);
    }

    @Test(timeout = 30_000)
    public void testLeaderElectionWithConfigurationOverrides() throws InterruptedException {
        // Starts Disabled but healthy, so leadership state should change to NoLeader.
        expectLocalLeadershipTransition(ClusterMemberLeadershipState.NonLeader, ACTIVE);

        // Now disable via configuration
        when(configuration.isLeaderElectionEnabled()).thenReturn(false);
        expectLocalLeadershipTransition(ClusterMemberLeadershipState.Disabled, INACTIVE);
    }

    private void expectLocalLeadershipTransition(ClusterMemberLeadershipState state) throws InterruptedException {
        LeaderElectionChangeEvent leadershipEvent = (LeaderElectionChangeEvent) eventSubscriber.takeNext(TIMEOUT);
        assertThat(leadershipEvent).isNotNull();
        assertThat(leadershipEvent.getLeadershipRevision().getCurrent().getLeadershipState()).isEqualTo(state);
        assertThat(service.getLocalLeadership().getCurrent().getLeadershipState()).isEqualTo(state);
    }

    private void expectLocalLeadershipTransition(ClusterMemberLeadershipState state, boolean active) throws InterruptedException {
        expectLocalLeadershipTransition(state);
        ClusterMembershipChangeEvent membershipChangeEvent = (ClusterMembershipChangeEvent) eventSubscriber.takeNext(TIMEOUT);
        assertThat(membershipChangeEvent).isNotNull();
        assertThat(membershipChangeEvent.getRevision().getCurrent().isActive()).isEqualTo(active);
    }
}