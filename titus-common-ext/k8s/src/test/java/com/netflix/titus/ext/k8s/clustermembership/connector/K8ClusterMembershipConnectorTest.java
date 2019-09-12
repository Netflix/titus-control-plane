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

package com.netflix.titus.ext.k8s.clustermembership.connector;

import java.time.Duration;
import java.util.function.Supplier;

import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadership;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadershipState;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipChangeEvent;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipEvent;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipSnapshotEvent;
import com.netflix.titus.api.clustermembership.model.event.LeaderElectionChangeEvent;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.testkit.model.clustermembership.ClusterMemberGenerator;
import com.netflix.titus.testkit.rx.TitusRxSubscriber;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.jayway.awaitility.Awaitility.await;
import static com.netflix.titus.testkit.model.clustermembership.ClusterMemberGenerator.activeClusterMember;
import static com.netflix.titus.testkit.model.clustermembership.ClusterMemberGenerator.clusterMemberRegistrationRevision;
import static com.netflix.titus.testkit.model.clustermembership.ClusterMemberGenerator.clusterMemberUpdateRevision;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class K8ClusterMembershipConnectorTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private static final ClusterMember LOCAL_MEMBER_UNREGISTERED = activeClusterMember("local").toBuilder()
            .withRegistered(false)
            .build();

    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    private final K8ConnectorConfiguration configuration = Archaius2Ext.newConfiguration(K8ConnectorConfiguration.class,
            "reRegistrationIntervalMs", "100"
    );

    private final StubbedK8Executors k8Executors = new StubbedK8Executors(LOCAL_MEMBER_UNREGISTERED.getMemberId());

    private K8ClusterMembershipConnector connector;

    private TitusRxSubscriber<ClusterMembershipEvent> connectorEvents = new TitusRxSubscriber<>();

    @Before
    public void setUp() {
        connector = new K8ClusterMembershipConnector(LOCAL_MEMBER_UNREGISTERED, k8Executors, k8Executors, configuration, titusRuntime);
        connector.membershipChangeEvents().subscribe(connectorEvents);

        // Initial sequence common for all tests
        assertThat(connector.getLocalClusterMemberRevision().getCurrent()).isEqualTo(LOCAL_MEMBER_UNREGISTERED);
        ClusterMembershipEvent snapshotEvent = connectorEvents.takeNext();
        assertThat(snapshotEvent).isInstanceOf(ClusterMembershipSnapshotEvent.class);
    }

    @After
    public void tearDown() {
        connector.shutdown();
    }

    @Test
    public void testRegistration() throws InterruptedException {
        long revision1 = connector.getLocalClusterMemberRevision().getRevision();

        // First registration
        long revision2 = doRegister().getRevision();

        // Re-registration
        long revision3 = doRegister().getRevision();

        assertThat(revision2).isGreaterThan(revision1);
        assertThat(revision3).isGreaterThan(revision2);
    }

    @Test
    public void testUnregistration() throws InterruptedException {
        // First cycle
        doRegister();
        doUnregister();

        // Second cycle
        doRegister();
        doUnregister();
    }

    @Test
    public void testRegistrationConnectionErrorInRegistrationRequest() throws InterruptedException {
        k8Executors.failOnMembershipUpdate(new RuntimeException("Simulated membership update error"), 1);
        try {
            connector.register(ClusterMemberGenerator::clusterMemberRegistrationRevision).block();
            fail("Failure expected");
        } catch (Exception ignore) {
        }
        doRegister();
    }

    @Test
    public void testRegistrationConnectionErrorRecoveryInReconciler() throws InterruptedException {
        doRegister();

        k8Executors.failOnMembershipUpdate(new RuntimeException("Simulated membership update error"), 1);
        await().until(() -> !k8Executors.isFailingOnMembershipUpdate());
        long now = System.currentTimeMillis();
        ClusterMembershipChangeEvent event;
        while ((event = (ClusterMembershipChangeEvent) connectorEvents.takeNext(TIMEOUT)) != null) {
            if (event.getRevision().getTimestamp() > now) {
                return;
            }
        }
        fail("No re-registration observed in time");
    }

    @Test
    public void testUnregistrationConnectionError() throws InterruptedException {
        doRegister();

        k8Executors.failOnMembershipUpdate(new RuntimeException("Simulated membership update error"), Integer.MAX_VALUE);
        try {
            connector.unregister(ClusterMemberGenerator::clusterMemberUnregistrationRevision).block();
            fail("Failure expected");
        } catch (Exception ignore) {
        }
        k8Executors.doNotFailOnMembershipUpdate();
        doUnregister();
    }

    @Test
    public void testSiblingAdded() throws InterruptedException {
        // Add some
        ClusterMembershipRevision<ClusterMember> sibling1 = doAddSibling("sibling1");
        ClusterMembershipRevision<ClusterMember> sibling2 = doAddSibling("sibling2");

        // Make updates
        doUpdateSibling(sibling1);
        doUpdateSibling(sibling2);

        // Now remove all siblings
        doRemoveSibling(sibling1);
        doRemoveSibling(sibling2);
    }

    @Test
    public void testMembershipEventStreamConnectionError() throws InterruptedException {
        k8Executors.breakMembershipEventSource();
        doRegister();
    }

    @Test
    public void testLeaderElection() throws InterruptedException {
        // Join leader election process
        joinLeaderElectionProcess();

        // Become leader
        electLocalAsLeader();

        // Stop being leader
        stopBeingLeader();
    }

    @Test
    public void testDiscoveryOfSiblingLeader() throws InterruptedException {
        ClusterMemberLeadership sibling = electSiblingAsLeader();
        ClusterMembershipRevision<ClusterMemberLeadership> leader = connector.findCurrentLeader().orElseThrow(() -> new IllegalStateException("Leader not found"));
        assertThat(leader.getCurrent()).isEqualTo(sibling);
    }

    @Test
    public void testJoinAndLeaveLeaderElectionProcess() throws InterruptedException {
        // Join leader election process
        joinLeaderElectionProcess();

        // Leave leader election process
        leaveLeaderElectionProcess();
    }

    @Test
    public void testLeadershipEventStreamConnectionError() throws InterruptedException {
        k8Executors.breakLeadershipEventSource();
        joinLeaderElectionProcess();
    }

    private ClusterMembershipRevision<ClusterMember> doRegister() throws InterruptedException {
        ClusterMembershipRevision<ClusterMember> newRevision = doRegistrationChange(() -> connector.register(ClusterMemberGenerator::clusterMemberRegistrationRevision).block());
        assertThat(connector.getLocalClusterMemberRevision().getCurrent().isRegistered()).isTrue();
        return newRevision;
    }

    private ClusterMembershipRevision<ClusterMember> doUnregister() throws InterruptedException {
        ClusterMembershipRevision<ClusterMember> newRevision = doRegistrationChange(() -> connector.unregister(ClusterMemberGenerator::clusterMemberUnregistrationRevision).block());
        assertThat(connector.getLocalClusterMemberRevision().getCurrent().isRegistered()).isFalse();
        return newRevision;
    }

    private ClusterMembershipRevision<ClusterMember> doRegistrationChange(Supplier<ClusterMembershipRevision<ClusterMember>> action) throws InterruptedException {
        long now = titusRuntime.getClock().wallTime();
        ClusterMembershipRevision<ClusterMember> newRevision = action.get();

        assertThat(newRevision.getTimestamp()).isGreaterThanOrEqualTo(now);
        assertThat(connector.getLocalClusterMemberRevision()).isEqualTo(newRevision);

        ClusterMembershipEvent registrationEvent1 = connectorEvents.takeNext(TIMEOUT);
        assertThat(registrationEvent1).isInstanceOf(ClusterMembershipChangeEvent.class);
        assertThat(((ClusterMembershipChangeEvent) registrationEvent1).getChangeType()).isEqualTo(ClusterMembershipChangeEvent.ChangeType.Updated);

        return newRevision;
    }

    private ClusterMembershipRevision<ClusterMember> doAddSibling(String siblingId) throws InterruptedException {
        ClusterMembershipRevision<ClusterMember> revision = clusterMemberRegistrationRevision(activeClusterMember(siblingId));
        return doAddOrUpdateSibling(revision, ClusterMembershipChangeEvent.ChangeType.Added);
    }

    private ClusterMembershipRevision<ClusterMember> doUpdateSibling(ClusterMembershipRevision<ClusterMember> revision) throws InterruptedException {
        return doAddOrUpdateSibling(clusterMemberUpdateRevision(revision), ClusterMembershipChangeEvent.ChangeType.Updated);
    }

    private ClusterMembershipRevision<ClusterMember> doAddOrUpdateSibling(ClusterMembershipRevision<ClusterMember> revision,
                                                                          ClusterMembershipChangeEvent.ChangeType changeType) throws InterruptedException {
        String siblingId = revision.getCurrent().getMemberId();
        k8Executors.addOrUpdateSibling(revision);

        ClusterMembershipEvent event = connectorEvents.takeNext(TIMEOUT);
        assertThat(event).isInstanceOf(ClusterMembershipChangeEvent.class);
        assertThat(((ClusterMembershipChangeEvent) event).getChangeType()).isEqualTo(changeType);
        assertThat(((ClusterMembershipChangeEvent) event).getRevision()).isEqualTo(revision);

        assertThat(connector.getClusterMemberSiblings()).containsKey(siblingId);

        return connector.getClusterMemberSiblings().get(siblingId);
    }

    private void doRemoveSibling(ClusterMembershipRevision<ClusterMember> revision) throws InterruptedException {
        k8Executors.removeSibling(revision.getCurrent().getMemberId());

        ClusterMembershipEvent event = connectorEvents.takeNext(TIMEOUT);
        assertThat(event).isInstanceOf(ClusterMembershipChangeEvent.class);
        assertThat(((ClusterMembershipChangeEvent) event).getChangeType()).isEqualTo(ClusterMembershipChangeEvent.ChangeType.Removed);
    }

    private void joinLeaderElectionProcess() throws InterruptedException {
        connector.joinLeadershipGroup().block();

        LeaderElectionChangeEvent electionEvent = takeLeaderElectionEvent(LeaderElectionChangeEvent.ChangeType.LocalJoined);
        assertThat(electionEvent.getLeadershipRevision().getCurrent().getLeadershipState()).isEqualTo(ClusterMemberLeadershipState.NonLeader);

        assertThat(connector.getLocalLeadershipRevision().getCurrent().getLeadershipState()).isEqualTo(ClusterMemberLeadershipState.NonLeader);
    }

    private void electLocalAsLeader() throws InterruptedException {
        k8Executors.emitLeadershipEvent(ClusterMembershipEvent.leaderElected(ClusterMembershipRevision.<ClusterMemberLeadership>newBuilder()
                .withCurrent(ClusterMemberLeadership.newBuilder()
                        .withMemberId(LOCAL_MEMBER_UNREGISTERED.getMemberId())
                        .withLeadershipState(ClusterMemberLeadershipState.Leader)
                        .build()
                )
                .build())
        );

        LeaderElectionChangeEvent electionEvent = takeLeaderElectionEvent(LeaderElectionChangeEvent.ChangeType.LeaderElected);
        assertThat(electionEvent.getLeadershipRevision().getCurrent().getLeadershipState()).isEqualTo(ClusterMemberLeadershipState.Leader);
    }

    private void leaveLeaderElectionProcess() throws InterruptedException {
        connector.leaveLeadershipGroup(true).block();

        LeaderElectionChangeEvent electionEvent = takeLeaderElectionEvent(LeaderElectionChangeEvent.ChangeType.LocalLeft);
        assertThat(electionEvent.getLeadershipRevision().getCurrent().getLeadershipState()).isEqualTo(ClusterMemberLeadershipState.Disabled);
        assertThat(connector.getLocalLeadershipRevision().getCurrent().getLeadershipState()).isEqualTo(ClusterMemberLeadershipState.Disabled);
    }

    private void stopBeingLeader() throws InterruptedException {
        connector.leaveLeadershipGroup(false).block();

        LeaderElectionChangeEvent electionEvent = takeLeaderElectionEvent(LeaderElectionChangeEvent.ChangeType.LeaderLost);
        assertThat(electionEvent.getLeadershipRevision().getCurrent().getLeadershipState()).isEqualTo(ClusterMemberLeadershipState.Disabled);
        assertThat(connector.getLocalLeadershipRevision().getCurrent().getLeadershipState()).isEqualTo(ClusterMemberLeadershipState.Disabled);
    }

    private ClusterMemberLeadership electSiblingAsLeader() throws InterruptedException {
        ClusterMemberLeadership sibling = ClusterMemberLeadership.newBuilder()
                .withMemberId("sibling1")
                .withLeadershipState(ClusterMemberLeadershipState.Leader)
                .build();
        k8Executors.emitLeadershipEvent(ClusterMembershipEvent.leaderElected(ClusterMembershipRevision.<ClusterMemberLeadership>newBuilder()
                .withCurrent(sibling)
                .build()
        ));

        LeaderElectionChangeEvent electionEvent = takeLeaderElectionEvent(LeaderElectionChangeEvent.ChangeType.LeaderElected);
        assertThat(electionEvent.getLeadershipRevision().getCurrent().getLeadershipState()).isEqualTo(ClusterMemberLeadershipState.Leader);

        return sibling;
    }

    private LeaderElectionChangeEvent takeLeaderElectionEvent(LeaderElectionChangeEvent.ChangeType changeType) throws InterruptedException {
        ClusterMembershipEvent event = connectorEvents.takeNext(TIMEOUT);
        assertThat(event).isInstanceOf(LeaderElectionChangeEvent.class);

        LeaderElectionChangeEvent electionEvent = (LeaderElectionChangeEvent) event;
        assertThat(electionEvent.getChangeType()).isEqualTo(changeType);

        return electionEvent;
    }
}