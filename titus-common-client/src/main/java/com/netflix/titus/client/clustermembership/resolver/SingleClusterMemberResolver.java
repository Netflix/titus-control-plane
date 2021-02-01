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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMemberAddress;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadership;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadershipState;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipFunctions;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipSnapshot;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.client.clustermembership.grpc.ReactorClusterMembershipClient;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.common.util.grpc.reactor.client.ReactorToGrpcClientBuilder;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.rx.ReactorRetriers;
import com.netflix.titus.common.util.rx.RetryHandlerBuilder;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.grpc.protogen.ClusterMember.LeadershipState;
import com.netflix.titus.grpc.protogen.ClusterMembershipEvent;
import com.netflix.titus.grpc.protogen.ClusterMembershipServiceGrpc;
import com.netflix.titus.runtime.common.grpc.GrpcClientErrorUtils;
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
import reactor.core.scheduler.Schedulers;

import static com.netflix.titus.api.clustermembership.model.ClusterMembershipFunctions.hasIpAddress;
import static com.netflix.titus.client.clustermembership.grpc.ClusterMembershipGrpcConverters.toCoreClusterMember;
import static com.netflix.titus.client.clustermembership.grpc.ClusterMembershipGrpcConverters.toCoreClusterMembershipRevision;

/**
 * Resolves cluster member addresses by calling a cluster membership service directly at a specified location.
 */
public class SingleClusterMemberResolver implements DirectClusterMemberResolver {

    private static final Logger logger = LoggerFactory.getLogger(SingleClusterMemberResolver.class);

    /**
     * Not used, but must be set on the builder.
     */
    private static final Duration GRPC_REQUEST_TIMEOUT = Duration.ofSeconds(1);

    private final String name;
    private final ClusterMembershipResolverConfiguration configuration;
    private final ClusterMemberAddress address;
    private final ClusterMemberVerifier clusterMemberVerifier;
    private final Clock clock;

    private final ManagedChannel channel;
    private final ReactorClusterMembershipClient client;
    private final Disposable eventStreamDisposable;

    private volatile String rejectedMemberError;

    private volatile Optional<String> connectedToMemberId = Optional.empty();
    private volatile ClusterMembershipSnapshot lastSnapshot = ClusterMembershipSnapshot.empty();

    private final ReplayProcessor<ClusterMembershipSnapshot> eventProcessor = ReplayProcessor.create(1);
    private final FluxSink<ClusterMembershipSnapshot> eventSink = eventProcessor.sink();

    private final AtomicReference<Long> disconnectTimeRef;

    public SingleClusterMemberResolver(ClusterMembershipResolverConfiguration configuration,
                                       Function<ClusterMemberAddress, ManagedChannel> channelProvider,
                                       ClusterMemberAddress address,
                                       ClusterMemberVerifier clusterMemberVerifier,
                                       Scheduler scheduler,
                                       TitusRuntime titusRuntime) {
        this.name = "member@" + ClusterMembershipFunctions.toStringUri(address);
        this.configuration = configuration;
        this.address = address;
        this.clusterMemberVerifier = clusterMemberVerifier;
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
                        logger.info("[{}] Unexpected end of stream. Converting to error to force retry...", name);
                        return Mono.error(new IllegalStateException(String.format("[%s] Unexpected end of stream", name)));
                    }
                    if (signal.getType() == SignalType.ON_ERROR) {
                        Throwable error = signal.getThrowable();
                        if (isConnectionDeadline(error)) {
                            logger.debug("[{}] Connection deadline", name, error);
                        } else {
                            logger.info("[{}] Connection terminated with an error: {}", name, GrpcClientErrorUtils.toDetailedMessage(error));
                            logger.debug("[{}] Stack trace", name, error);
                        }
                        disconnectTimeRef.compareAndSet(null, clock.wallTime());
                        return Mono.error(error);
                    }
                    if (signal.getType() == SignalType.ON_NEXT) {
                        ClusterMembershipEvent event = signal.get();
                        if (event.getEventCase() == ClusterMembershipEvent.EventCase.SNAPSHOT) {
                            this.rejectedMemberError = checkSnapshot(event.getSnapshot()).orElse(null);
                            if (rejectedMemberError != null) {
                                logger.error(rejectedMemberError);
                                return Mono.error(new IllegalStateException(rejectedMemberError));
                            }
                        }
                        return Mono.just(event);
                    }
                    return Mono.empty();
                })
                .retryWhen(ReactorRetriers.rectorPredicateRetryer(this::isConnectionDeadline))
                .retryWhen(RetryHandlerBuilder.retryHandler()
                        .withUnlimitedRetries()
                        .withDelay(configuration.getSingleMemberInitialRetryIntervalMs(), configuration.getSingleMemberMaxRetryIntervalMs(), TimeUnit.MILLISECONDS)
                        .withReactorScheduler(Schedulers.parallel())
                        .buildRetryExponentialBackoff()
                )
                .doFinally(signal -> {
                    if (signal == SignalType.CANCEL) {
                        logger.info("[{}] Event stream canceled: address={}", name, address);
                    }
                    disconnectTimeRef.compareAndSet(null, clock.wallTime());
                })
                .subscribe(
                        next -> {
                            disconnectTimeRef.set(null);
                            processEvent(next);
                        },
                        e -> logger.warn("[{}] Event stream terminated with an error: address={}", name, address, e),
                        () -> logger.info("[{}] Event stream terminated: address={}", name, address)
                );
    }

    private Optional<String> checkSnapshot(ClusterMembershipEvent.Snapshot snapshot) {
        if (CollectionsExt.isNullOrEmpty(snapshot.getRevisionsList())) {
            return Optional.of(String.format("[%s] Empty cluster membership list", name));
        }

        ClusterMember member = toCoreClusterMember(snapshot.getRevisionsList().get(0).getCurrent());
        ClusterMemberVerifierResult result = clusterMemberVerifier.verify(member);
        return result.isValid() ? Optional.empty() : Optional.of(result.getMessage());
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
        return disconnectTimeRef.get() == null || !clock.isPast(disconnectTimeRef.get() + configuration.getHealthDisconnectThresholdMs());
    }

    @Override
    public ClusterMembershipSnapshot getSnapshot() {
        return lastSnapshot;
    }

    @Override
    public Flux<ClusterMembershipSnapshot> resolve() {
        return eventProcessor;
    }

    @VisibleForTesting
    String getRejectedMemberError() {
        return rejectedMemberError;
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
        snapshot.getRevisionsList().forEach(grpcRevision -> {
            ClusterMembershipRevision<ClusterMember> coreRevision = toCoreClusterMembershipRevision(grpcRevision);
            builder.withMemberRevisions(coreRevision);
            boolean isLeader = grpcRevision.getCurrent().getLeadershipState() == LeadershipState.Leader;
            if (isLeader) {
                builder.withLeaderRevision(buildLeaderRevision(coreRevision));
            }
        });
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
                builder.withLeaderRevision(buildLeaderRevision(memberRevision));
            }
        } else if (previousLeader) {
            builder.withLeaderRevision(null);
        }

        return builder.build();
    }

    private ClusterMembershipRevision<ClusterMemberLeadership> buildLeaderRevision(ClusterMembershipRevision<ClusterMember> memberRevision) {
        return ClusterMembershipRevision.<ClusterMemberLeadership>newBuilder()
                .withCurrent(ClusterMemberLeadership.newBuilder()
                        .withMemberId(memberRevision.getCurrent().getMemberId())
                        .withLeadershipState(ClusterMemberLeadershipState.Leader)
                        .build()
                )
                .withRevision(memberRevision.getRevision())
                .withTimestamp(memberRevision.getTimestamp())
                .build();
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
        if (error == null) {
            return false;
        }
        if (!(error instanceof StatusRuntimeException)) {
            return false;
        }
        StatusRuntimeException statusException = (StatusRuntimeException) error;
        return statusException.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED;
    }
}
