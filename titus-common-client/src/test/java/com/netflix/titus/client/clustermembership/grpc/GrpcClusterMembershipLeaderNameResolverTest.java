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

package com.netflix.titus.client.clustermembership.grpc;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;

import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipSnapshot;
import com.netflix.titus.client.clustermembership.resolver.ClusterMemberResolver;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.tuple.Either;
import com.netflix.titus.testkit.rx.TitusRxSubscriber;
import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.Status;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;

import static com.jayway.awaitility.Awaitility.await;
import static com.netflix.titus.testkit.model.clustermembership.ClusterMemberGenerator.activeClusterMember;
import static com.netflix.titus.testkit.model.clustermembership.ClusterMemberGenerator.clusterMemberRegistrationRevision;
import static com.netflix.titus.testkit.model.clustermembership.ClusterMemberGenerator.leaderRevision;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GrpcClusterMembershipLeaderNameResolverTest {

    private static final ClusterMembershipRevision<ClusterMember> MEMBER_1 = clusterMemberRegistrationRevision(activeClusterMember("member1", "10.0.0.1"));
    private static final ClusterMembershipRevision<ClusterMember> MEMBER_2 = clusterMemberRegistrationRevision(activeClusterMember("member2", "10.0.0.2"));

    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private final GrpcClusterMembershipNameResolverConfiguration configuration = mock(GrpcClusterMembershipNameResolverConfiguration.class);

    private final ClusterMemberResolver clusterResolver = mock(ClusterMemberResolver.class);

    private GrpcClusterMembershipLeaderNameResolver nameResolver;

    private volatile DirectProcessor<ClusterMembershipSnapshot> clusterEventProcessor;

    private final TitusRxSubscriber<Either<List<EquivalentAddressGroup>, Status>> nameResolverSubscriber = new TitusRxSubscriber<>();

    @Before
    public void setUp() {
        when(clusterResolver.resolve()).thenReturn(Flux.defer(() -> clusterEventProcessor = DirectProcessor.create()));

        nameResolver = new GrpcClusterMembershipLeaderNameResolver(
                configuration,
                clusterResolver,
                member -> member.getClusterMemberAddresses().get(0)
        );

        Flux.<Either<List<EquivalentAddressGroup>, Status>>create(sink -> {
            nameResolver.start(new NameResolver.Listener() {
                @Override
                public void onAddresses(List<EquivalentAddressGroup> servers, Attributes attributes) {
                    sink.next(Either.ofValue(servers));
                }

                @Override
                public void onError(Status error) {
                    sink.next(Either.ofError(error));
                }
            });
            sink.onCancel(() -> nameResolver.shutdown());
        }).subscribe(nameResolverSubscriber);
    }

    @After
    public void tearDown() {
        ReactorExt.safeDispose(nameResolverSubscriber);
    }

    @Test
    public void testLeaderSelection() throws InterruptedException {
        // No members yet
        clusterEventProcessor.onNext(ClusterMembershipSnapshot.empty());
        expectError();

        // Add member but not leader
        clusterEventProcessor.onNext(ClusterMembershipSnapshot.newBuilder().withMemberRevisions(MEMBER_1).build());
        expectError();

        // Add another member, which is also a leader
        clusterEventProcessor.onNext(ClusterMembershipSnapshot.newBuilder()
                .withMemberRevisions(MEMBER_1, MEMBER_2)
                .withLeaderRevision(leaderRevision(MEMBER_2))
                .build());
        expectLeader(MEMBER_2);
    }

    @Test
    public void testRetryOnError() throws InterruptedException {
        testRetryOnTerminatedStream(() -> clusterEventProcessor.onError(new RuntimeException("simulated error")));
    }

    @Test
    public void testRetryWhenCompleted() throws InterruptedException {
        testRetryOnTerminatedStream(() -> clusterEventProcessor.onComplete());
    }

    private void testRetryOnTerminatedStream(Runnable breakAction) throws InterruptedException {
        clusterEventProcessor.onNext(ClusterMembershipSnapshot.empty());
        assertThat(nameResolverSubscriber.takeNext(TIMEOUT)).isNotNull();

        DirectProcessor<ClusterMembershipSnapshot> currentProcessor = clusterEventProcessor;
        breakAction.run();
        await().until(() -> clusterEventProcessor != currentProcessor);

        clusterEventProcessor.onNext(ClusterMembershipSnapshot.empty());
        assertThat(nameResolverSubscriber.takeNext(TIMEOUT)).isNotNull();
    }

    private void expectLeader(ClusterMembershipRevision<ClusterMember> leaderMember) throws InterruptedException {
        Either<List<EquivalentAddressGroup>, Status> event = nameResolverSubscriber.takeNext(TIMEOUT);
        assertThat(event).isNotNull();
        assertThat(event.hasValue()).isTrue();
        assertThat(event.getValue()).hasSize(1);

        EquivalentAddressGroup addressGroup = event.getValue().get(0);
        assertThat(addressGroup.getAddresses()).hasSize(1);
        InetSocketAddress ipAddress = (InetSocketAddress) addressGroup.getAddresses().get(0);
        assertThat(ipAddress.getHostString()).isEqualTo(leaderMember.getCurrent().getClusterMemberAddresses().get(0).getIpAddress());
    }

    private void expectError() throws InterruptedException {
        Either<List<EquivalentAddressGroup>, Status> event = nameResolverSubscriber.takeNext(TIMEOUT);
        assertThat(event).isNotNull();
        assertThat(event.hasError()).isTrue();
        assertThat(event.getError().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    }
}