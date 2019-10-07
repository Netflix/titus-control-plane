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

package com.netflix.titus.client.clustermembership.resolver;

import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;

import com.netflix.titus.api.clustermembership.model.ClusterMemberAddress;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipSnapshot;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.grpc.protogen.ClusterMember;
import com.netflix.titus.grpc.protogen.ClusterMembershipRevision;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.core.scheduler.Schedulers;

import static com.jayway.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

public class SingleClusterMemberResolverTest {

    private static final String IP_PREFIX = "1.0.0.";

    private static final ClusterMemberAddress ADDRESS = ClusterMemberAddress.newBuilder()
            .withIpAddress(IP_PREFIX + "1")
            .withProtocol("grpc")
            .build();

    private static final java.time.Duration TIMEOUT = Duration.ofSeconds(5);

    private static final ClusterMembershipRevision MEMBER_1 = newMember(1);
    private static final ClusterMembershipRevision MEMBER_2 = newMember(2);
    private static final ClusterMembershipRevision MEMBER_3 = newMember(3);

    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    private final ClusterMembershipResolverConfiguration configuration = Archaius2Ext.newConfiguration(ClusterMembershipResolverConfiguration.class);

    private final GrpcClusterMembershipServiceStub serviceStub = new GrpcClusterMembershipServiceStub();

    private Server server;

    private SingleClusterMemberResolver resolver;

    @Before
    public void setUp() throws IOException {
        serviceStub.addMember(MEMBER_1.toBuilder()
                .setCurrent(MEMBER_1.getCurrent().toBuilder()
                        .setLeadershipState(ClusterMember.LeadershipState.Leader)
                )
                .build()
        );
        serviceStub.addMember(MEMBER_2);
        serviceStub.addMember(MEMBER_3);

        String serviceName = "clusterMembershipService#" + System.currentTimeMillis();
        this.server = InProcessServerBuilder.forName(serviceName)
                .directExecutor()
                .addService(serviceStub)
                .build()
                .start();

        this.resolver = new SingleClusterMemberResolver(
                configuration,
                address -> InProcessChannelBuilder.forName(serviceName).directExecutor().build(),
                ADDRESS,
                Schedulers.parallel(),
                titusRuntime
        );

        // Starts with a healthy connection.
        await().until(() -> resolver.getPrintableName().equals(MEMBER_1.getCurrent().getMemberId()));
    }

    @After
    public void tearDown() {
        ExceptionExt.silent(resolver, SingleClusterMemberResolver::shutdown);
        ExceptionExt.silent(server, Server::shutdownNow);
    }

    @Test(timeout = 30_000)
    public void testInitialState() {
        assertThat(resolver.isHealthy()).isTrue();
        assertThat(resolver.getAddress()).isEqualTo(ADDRESS);

        ClusterMembershipSnapshot snapshotEvent = resolver.resolve().blockFirst(TIMEOUT);
        assertThat(snapshotEvent).isNotNull();
        assertThat(snapshotEvent.getMemberRevisions()).hasSize(3);
        assertThat(snapshotEvent.getLeaderRevision()).isNotEmpty();
    }

    @Test(timeout = 30_000)
    public void testMemberUpdate() {
        Iterator<ClusterMembershipSnapshot> eventIt = subscribeAndDiscardFirstSnapshot();

        serviceStub.addMember(MEMBER_1.toBuilder()
                .setMessage("Update")
                .build()
        );

        ClusterMembershipSnapshot update = eventIt.next();
        com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision<com.netflix.titus.api.clustermembership.model.ClusterMember> updated =
                update.getMemberRevisions().get(MEMBER_1.getCurrent().getMemberId());
        assertThat(updated.getMessage()).isEqualTo("Update");
    }

    @Test(timeout = 30_000)
    public void testMemberRemoval() {
        Iterator<ClusterMembershipSnapshot> eventIt = subscribeAndDiscardFirstSnapshot();

        serviceStub.removeMember(MEMBER_2);

        ClusterMembershipSnapshot update = eventIt.next();
        assertThat(update.getMemberRevisions()).hasSize(2);
        assertThat(update.getMemberRevisions()).doesNotContainKey(MEMBER_2.getCurrent().getMemberId());
    }

    @Test(timeout = 30_000)
    public void testLeaderElection() {
        Iterator<ClusterMembershipSnapshot> eventIt = subscribeAndDiscardFirstSnapshot();

        // Elect a leader
        serviceStub.addMember(MEMBER_2.toBuilder()
                .setCurrent(MEMBER_2.getCurrent().toBuilder()
                        .setLeadershipState(ClusterMember.LeadershipState.Leader)
                )
                .build()
        );
        ClusterMembershipSnapshot leadElectedEvent = eventIt.next();
        assertThat(leadElectedEvent.getLeaderRevision()).isNotEmpty();

        // Loose the leader.
        serviceStub.removeMember(MEMBER_2);
        ClusterMembershipSnapshot leaderLostEvent = eventIt.next();
        assertThat(leaderLostEvent.getLeaderRevision()).isEmpty();
    }

    @Test
    public void testElectedLeaderSetInSnapshot() {
        Iterator<ClusterMembershipSnapshot> eventIt = resolver.resolve().toIterable().iterator();
        ClusterMembershipSnapshot leadElectedEvent = eventIt.next();

        assertThat(leadElectedEvent.getLeaderRevision()).isNotEmpty();
    }

    @Test(timeout = 30_000)
    public void testBrokenConnection() {
        Iterator<ClusterMembershipSnapshot> eventIt = subscribeAndDiscardFirstSnapshot();

        serviceStub.breakConnection();
        ClusterMembershipSnapshot newSnapshot = eventIt.next();
        assertThat(newSnapshot).isNotNull();
        assertThat(newSnapshot.getMemberRevisions()).hasSize(3);
    }

    @Test(timeout = 30_000)
    public void testReconnectOnStreamOnComplete() {
        Iterator<ClusterMembershipSnapshot> eventIt = subscribeAndDiscardFirstSnapshot();

        serviceStub.completeEventStream();
        ClusterMembershipSnapshot newSnapshot = eventIt.next();
        assertThat(newSnapshot).isNotNull();
        assertThat(newSnapshot.getMemberRevisions()).hasSize(3);
    }

    private Iterator<ClusterMembershipSnapshot> subscribeAndDiscardFirstSnapshot() {
        // Subscribe and consume first snapshot
        Iterator<ClusterMembershipSnapshot> eventIt = resolver.resolve().toIterable().iterator();
        eventIt.next();
        return eventIt;
    }

    private static ClusterMembershipRevision newMember(int idx) {
        return ClusterMembershipRevision.newBuilder()
                .setCurrent(ClusterMember.newBuilder()
                        .setMemberId("member" + idx)
                        .addAddresses(com.netflix.titus.grpc.protogen.ClusterMemberAddress.newBuilder()
                                .setIpAddress(IP_PREFIX + idx)
                        )
                        .setLeadershipState(ClusterMember.LeadershipState.NonLeader
                        )
                )
                .build();
    }
}
