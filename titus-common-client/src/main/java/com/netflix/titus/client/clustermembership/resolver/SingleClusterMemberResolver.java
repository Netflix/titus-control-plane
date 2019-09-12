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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMemberAddress;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadership;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadershipState;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipSnapshot;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.client.clustermembership.grpc.ReactorClusterMembershipClient;
import com.netflix.titus.runtime.common.grpc.GrpcClientErrorUtils;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.grpc.protogen.ClusterMember.LeadershipState;
import com.netflix.titus.grpc.protogen.ClusterMembershipEvent;
import com.netflix.titus.grpc.protogen.ClusterMembershipServiceGrpc;
import com.netflix.titus.common.util.grpc.reactor.client.ReactorToGrpcClientBuilder;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;

import static com.netflix.titus.api.clustermembership.model.ClusterMembershipFunctions.hasIpAddress;
import static com.netflix.titus.client.clustermembership.grpc.ClusterMembershipGrpcConverters.toCoreClusterMembershipRevision;

/**
 * TODO Move to titus-common-client package once all the necessary dependencies are moved.
 * <p>
 * Resolves cluster member addresses by calling a cluster membership service directly at a specified location.
 */
class SingleClusterMemberResolver implements DirectClusterMemberResolver {

    private static final Logger logger = LoggerFactory.getLogger(SingleClusterMemberResolver.class);

    /**
     * Not used, but must be set on the builder.
     */
    private static final Duration GRPC_REQUEST_TIMEOUT = Duration.ofSeconds(1);

    private final ClusterMembershipResolverConfiguration configuration;
    private final ClusterMemberAddress address;
    private final Clock clock;

    private final ManagedChannel channel;
    private final ReactorClusterMembershipClient client;
    private final Disposable eventStreamDisposable;

    private volatile Optional<String> connectedToMemberId = Optional.empty();
    private volatile ClusterMembershipSnapshot lastSnapshot = ClusterMembershipSnapshot.empty();

    private final ReplayProcessor<ClusterMembershipSnapshot> eventProcessor = ReplayProcessor.create(1);
    private final FluxSink<ClusterMembershipSnapshot> eventSink = eventProcessor.sink();

    private final AtomicReference<Long> disconnectTimeRef;


    public SingleClusterMemberResolver(ClusterMembershipResolverConfiguration configuration,
                                       Function<ClusterMemberAddress, ManagedChannel> channelProvider,
                                       ClusterMemberAddress address,
                                       Scheduler scheduler,
                                       TitusRuntime titusRuntime) {
        this.configuration = configuration;
        this.address = address;
        this.clock = titusRuntime.getClock();

        this.channel = channelProvider.apply(address);
        this.disconnectTimeRef = new AtomicReference<>(clock.wallTime());
        this.client = ReactorToGrpcClientBuilder
                .newBuilderWithDefaults(
                        ReactorClusterMembershipClient.class,
                        ClusterMembershipServiceGrpc.newStub(channel),
                        ClusterMembershipServiceGrpc.getServiceDescriptor(),
                        CallMetadata.class
                )
                // FIXME Once call metadata interceptor is moved into common module
//                .withGrpcStubDecorator(AnonymousCallMetadataResolver.getInstance())
                .withTimeout(GRPC_REQUEST_TIMEOUT)
                .withStreamingTimeout(Duration.ofMillis(configuration.getSingleMemberReconnectIntervalMs()))
                .build();

        this.eventStreamDisposable = client.events()
                .materialize()
                .flatMap(signal -> {
                    if (signal.getType() == SignalType.ON_COMPLETE) {
                        logger.info("Unexpected end of stream. Converting to error to force retry...");
                        return Mono.error(new IllegalStateException("Unexpected end of stream"));
                    }
                    if (signal.getType() == SignalType.ON_ERROR) {
                        Throwable error = signal.getThrowable();
                        logger.info("Connection terminated with an error: {}", GrpcClientErrorUtils.toDetailedMessage(error));
                        logger.debug("Stack trace", error);
                        disconnectTimeRef.set(clock.wallTime());
                        return Mono.error(error);
                    }
                    if (signal.getType() == SignalType.ON_NEXT) {
                        return Mono.just(signal.get());
                    }
                    return Mono.empty();
                })
                .retry(this::isConnectionDeadline)
                .retryBackoff(Long.MAX_VALUE,
                        Duration.ofMillis(configuration.getSingleMemberInitialRetryIntervalMs()),
                        Duration.ofMillis(configuration.getSingleMemberMaxRetryIntervalMs()),
                        scheduler
                )
                .doFinally(signal -> disconnectTimeRef.set(clock.wallTime()))
                .subscribe(
                        next -> {
                            disconnectTimeRef.set(null);
                            processEvent(next);
                        },
                        e -> logger.warn("Event stream terminated with an error: address={}", address, e),
                        () -> logger.info("Event stream terminated: address={}", address)
                );
    }

    @Override
    public void shutdown() {
        ReactorExt.safeDispose(eventStreamDisposable);
        ExceptionExt.silent(eventSink::complete);
        ExceptionExt.silent(channel::shutdownNow);
    }

    @Override
    public ClusterMemberAddress getAddress() {
        return address;
    }

    @Override
    public String getPrintableName() {
        return connectedToMemberId.orElse("<connecting>");
    }

    @Override
    public boolean isHealthy() {
        return disconnectTimeRef.get() == null || clock.isPast(disconnectTimeRef.get() + configuration.getHealthDisconnectThresholdMs());
    }

    @Override
    public ClusterMembershipSnapshot getSnapshot() {
        return lastSnapshot;
    }

    @Override
    public Flux<ClusterMembershipSnapshot> resolve() {
        return eventProcessor;
    }

    private void processEvent(ClusterMembershipEvent next) {
        ClusterMembershipSnapshot newSnapshot;
        switch (next.getEventCase()) {
            case SNAPSHOT:
                newSnapshot = processSnapshotEvent(next.getSnapshot());
                break;
            case MEMBERUPDATED:
                newSnapshot = processMemberUpdateEvent(next.getMemberUpdated());
                break;
            case MEMBERREMOVED:
                newSnapshot = processMemberRemovedEvent(next.getMemberRemoved());
                break;
            default:
                return;
        }
        this.lastSnapshot = newSnapshot;

        if (!connectedToMemberId.isPresent()) {
            newSnapshot.getMemberRevisions().values().stream()
                    .filter(m -> hasIpAddress(m.getCurrent(), address.getIpAddress()))
                    .findFirst()
                    .ifPresent(m -> this.connectedToMemberId = Optional.of(m.getCurrent().getMemberId()));
        }

        eventSink.next(lastSnapshot);
    }

    private ClusterMembershipSnapshot processSnapshotEvent(ClusterMembershipEvent.Snapshot snapshot) {
        ClusterMembershipSnapshot.Builder builder = ClusterMembershipSnapshot.newBuilder();
        snapshot.getRevisionsList().forEach(grpcRevision -> builder.withMemberRevisions(toCoreClusterMembershipRevision(grpcRevision)));
        return builder.build();
    }

    private ClusterMembershipSnapshot processMemberUpdateEvent(ClusterMembershipEvent.MemberUpdated memberUpdated) {
        ClusterMembershipRevision<ClusterMember> memberRevision = toCoreClusterMembershipRevision(memberUpdated.getRevision());
        ClusterMembershipSnapshot.Builder builder = lastSnapshot.toBuilder().withMemberRevisions(memberRevision);

        String memberId = memberUpdated.getRevision().getCurrent().getMemberId();
        boolean isLeader = memberUpdated.getRevision().getCurrent().getLeadershipState() == LeadershipState.Leader;
        boolean previousLeader = lastSnapshot.getLeaderRevision().map(r -> r.getCurrent().getMemberId().equals(memberId)).orElse(false);

        if (isLeader) {
            if (!previousLeader) {
                builder.withLeaderRevision(ClusterMembershipRevision.<ClusterMemberLeadership>newBuilder()
                        .withCurrent(ClusterMemberLeadership.newBuilder()
                                .withMemberId(memberId)
                                .withLeadershipState(ClusterMemberLeadershipState.Leader)
                                .build()
                        )
                        .withRevision(memberRevision.getRevision())
                        .withTimestamp(memberRevision.getTimestamp())
                        .build()
                );
            }
        } else if (previousLeader) {
            builder.withLeaderRevision(null);
        }

        return builder.build();
    }

    private ClusterMembershipSnapshot processMemberRemovedEvent(ClusterMembershipEvent.MemberRemoved memberRemoved) {
        ClusterMembershipRevision<ClusterMember> memberRevision = toCoreClusterMembershipRevision(memberRemoved.getRevision());
        ClusterMembershipSnapshot.Builder builder = lastSnapshot.toBuilder().withoutMemberRevisions(memberRevision);

        String memberId = memberRemoved.getRevision().getCurrent().getMemberId();
        boolean previousLeader = lastSnapshot.getLeaderRevision().map(r -> r.getCurrent().getMemberId().equals(memberId)).orElse(false);
        if (previousLeader) {
            builder.withLeaderRevision(null);
        }

        return builder.build();
    }

    private boolean isConnectionDeadline(Throwable error) {
        if (!(error instanceof StatusRuntimeException)) {
            return false;
        }
        StatusRuntimeException statusException = (StatusRuntimeException) error;
        return statusException.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED;
    }
}
