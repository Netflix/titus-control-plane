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

import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMemberAddress;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadership;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipSnapshot;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.ReplayProcessor;

import static com.jayway.awaitility.Awaitility.await;
import static com.netflix.titus.testkit.model.clustermembership.ClusterMemberGenerator.activeClusterMember;
import static com.netflix.titus.testkit.model.clustermembership.ClusterMemberGenerator.clusterMemberRegistrationRevision;
import static com.netflix.titus.testkit.model.clustermembership.ClusterMemberGenerator.leaderRevision;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MultiNodeClusterMemberResolverTest {

    private static final java.time.Duration TIMEOUT = Duration.ofSeconds(5);

    private static final ClusterMembershipRevision<ClusterMember> MEMBER_1 = clusterMemberRegistrationRevision(activeClusterMember("member1", "10.0.0.1"));
    private static final ClusterMembershipRevision<ClusterMember> MEMBER_2 = clusterMemberRegistrationRevision(activeClusterMember("member2", "10.0.0.2"));
    private static final ClusterMembershipRevision<ClusterMember> MEMBER_3 = clusterMemberRegistrationRevision(activeClusterMember("member3", "10.0.0.3"));

    private final TitusRuntime titusRuntime = TitusRuntimes.internal(Duration.ofMillis(1));

    private final ClusterMembershipResolverConfiguration configuration = mock(ClusterMembershipResolverConfiguration.class);

    private final TestableDirectClusterMemberResolver memberResolver1 = new TestableDirectClusterMemberResolver(MEMBER_1);
    private final TestableDirectClusterMemberResolver memberResolver2 = new TestableDirectClusterMemberResolver(MEMBER_2);
    private final TestableDirectClusterMemberResolver memberResolver3 = new TestableDirectClusterMemberResolver(MEMBER_3);

    private MultiNodeClusterMemberResolver resolver;

    private final ConcurrentMap<String, ClusterMemberAddress> seedAddresses = new ConcurrentHashMap<>();

    @Before
    public void setUp() {
        when(configuration.getMultiMemberRefreshIntervalMs()).thenReturn(10L);

        addSeed(MEMBER_1);

        this.resolver = new MultiNodeClusterMemberResolver(
                configuration,
                () -> new HashSet<>(seedAddresses.values()),
                address -> {
                    if (isSameIpAddress(address, MEMBER_1)) {
                        return memberResolver1;
                    }
                    if (isSameIpAddress(address, MEMBER_2)) {
                        return memberResolver2;
                    }
                    if (isSameIpAddress(address, MEMBER_3)) {
                        return memberResolver3;
                    }
                    throw new IllegalStateException("unrecognized address: " + address);
                },
                member -> member.getClusterMemberAddresses().get(0),
                titusRuntime
        );

        memberResolver1.addMembers(MEMBER_1, MEMBER_2);
        memberResolver2.addMembers(MEMBER_2);
        memberResolver3.addMembers(MEMBER_3);
    }

    @Test(timeout = 30_000)
    public void testInitialState() {
        await().until(() -> resolver.getSnapshot().getMemberRevisions().size() == 2);
        ClusterMembershipSnapshot snapshot = resolver.resolve().blockFirst(TIMEOUT);
        assertThat(snapshot).isNotNull();
        assertThat(snapshot.getMemberRevisions()).hasSize(2);
        assertThat(snapshot.getLeaderRevision()).isEmpty();
        assertThat(snapshot.getMemberRevisions().keySet()).contains("member1", "member2");
    }

    @Test(timeout = 30_000)
    public void testNewMemberDiscovery() {
        Iterator<ClusterMembershipSnapshot> eventIterator = resolver.resolve().toIterable().iterator();
        eventIterator.next();

        memberResolver1.addMembers(MEMBER_3);
        await().until(() -> resolver.getSnapshot().getMemberRevisions().size() == 3);
        ClusterMembershipSnapshot nextEvent = eventIterator.next();
        assertThat(nextEvent.getMemberRevisions()).hasSize(3);
    }

    @Test
    public void testSeedUpdate() {
        Iterator<ClusterMembershipSnapshot> eventIterator = resolver.resolve().toIterable().iterator();
        eventIterator.next();

        addSeed(MEMBER_3);
        await().until(() -> resolver.getSnapshot().getMemberRevisions().size() == 3);
        ClusterMembershipSnapshot nextEvent = eventIterator.next();
        assertThat(nextEvent.getMemberRevisions()).hasSize(3);
    }

    @Test(timeout = 30_000)
    public void testMemberUpdate() {
        Iterator<ClusterMembershipSnapshot> eventIterator = resolver.resolve().toIterable().iterator();
        eventIterator.next();

        long initialRevision = MEMBER_1.getRevision();
        long nextRevision = initialRevision + 1;

        memberResolver1.addMembers(MEMBER_1.toBuilder().withRevision(nextRevision).build());
        ClusterMembershipSnapshot nextEvent = eventIterator.next();
        assertThat(nextEvent.getMemberRevisions().get(MEMBER_1.getCurrent().getMemberId()).getRevision()).isEqualTo(nextRevision);
    }

    @Test(timeout = 30_000)
    public void testMemberRemoval() {
        Iterator<ClusterMembershipSnapshot> eventIterator = resolver.resolve().toIterable().iterator();
        eventIterator.next();

        memberResolver1.removeMembers(MEMBER_2.getCurrent().getMemberId());
        memberResolver2.changeHealth(false);

        ClusterMembershipSnapshot nextEvent = eventIterator.next();
        assertThat(nextEvent.getMemberRevisions()).hasSize(1);
    }

    @Test(timeout = 30_000)
    public void testLeaderElection() {
        Iterator<ClusterMembershipSnapshot> eventIterator = resolver.resolve().toIterable().iterator();
        eventIterator.next();

        // Make member 1 the leader
        memberResolver1.setAsLeader(MEMBER_1);
        Optional<ClusterMembershipRevision<ClusterMemberLeadership>> firstLeader = eventIterator.next().getLeaderRevision();
        assertThat(firstLeader).isPresent();
        assertThat(firstLeader.get().getCurrent().getMemberId()).isEqualTo(MEMBER_1.getCurrent().getMemberId());

        // Make member 2 the leader
        memberResolver1.setAsLeader(MEMBER_2);
        Optional<ClusterMembershipRevision<ClusterMemberLeadership>> secondLeader = eventIterator.next().getLeaderRevision();
        assertThat(secondLeader).isPresent();
        assertThat(secondLeader.get().getCurrent().getMemberId()).isEqualTo(MEMBER_2.getCurrent().getMemberId());
    }

    private void addSeed(ClusterMembershipRevision<ClusterMember> seedMember) {
        seedAddresses.put(seedMember.getCurrent().getMemberId(), seedMember.getCurrent().getClusterMemberAddresses().get(0));
    }

    private boolean isSameIpAddress(ClusterMemberAddress address, ClusterMembershipRevision<ClusterMember> memberRevision) {
        return address.getIpAddress().equals(memberRevision.getCurrent().getClusterMemberAddresses().get(0).getIpAddress());
    }

    private static class TestableDirectClusterMemberResolver implements DirectClusterMemberResolver {

        private final ClusterMembershipRevision<ClusterMember> memberRevision;

        private final ConcurrentMap<String, ClusterMembershipRevision<ClusterMember>> memberRevisionsById = new ConcurrentHashMap<>();
        private volatile Optional<ClusterMembershipRevision<ClusterMemberLeadership>> currentLeaderOptional = Optional.empty();
        private volatile boolean healthStatus = true;

        private final ReplayProcessor<ClusterMembershipSnapshot> snapshotEventProcessor = ReplayProcessor.create(1);
        private final FluxSink<ClusterMembershipSnapshot> snapshotFluxSink = snapshotEventProcessor.sink();

        private TestableDirectClusterMemberResolver(ClusterMembershipRevision<ClusterMember> memberRevision) {
            this.memberRevision = memberRevision;
        }

        @Override
        public ClusterMemberAddress getAddress() {
            return memberRevision.getCurrent().getClusterMemberAddresses().get(0);
        }

        @Override
        public String getPrintableName() {
            return memberRevision.getCurrent().getMemberId();
        }

        @Override
        public boolean isHealthy() {
            return healthStatus;
        }

        @Override
        public void shutdown() {
        }

        @Override
        public ClusterMembershipSnapshot getSnapshot() {
            return newSnapshot();
        }

        private ClusterMembershipSnapshot newSnapshot() {
            ClusterMembershipSnapshot.Builder builder = ClusterMembershipSnapshot.newBuilder().withMemberRevisions(memberRevisionsById.values());
            currentLeaderOptional.ifPresent(builder::withLeaderRevision);
            return builder.build();
        }

        @Override
        public Flux<ClusterMembershipSnapshot> resolve() {
            return snapshotEventProcessor;
        }

        @SafeVarargs
        private final void addMembers(ClusterMembershipRevision<ClusterMember>... members) {
            for (ClusterMembershipRevision<ClusterMember> member : members) {
                memberRevisionsById.put(member.getCurrent().getMemberId(), member);
            }
            snapshotFluxSink.next(newSnapshot());
        }

        private void removeMembers(String... ids) {
            for (String id : ids) {
                memberRevisionsById.remove(id);
            }
            snapshotFluxSink.next(newSnapshot());
        }

        private void setAsLeader(ClusterMembershipRevision<ClusterMember> member) {
            currentLeaderOptional = Optional.of(leaderRevision(member));
        }

        private void changeHealth(boolean healthStatus) {
            this.healthStatus = healthStatus;
        }
    }
}