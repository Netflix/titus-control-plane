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
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMemberAddress;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipSnapshot;
import com.netflix.titus.client.clustermembership.resolver.ClusterMemberResolver;
import com.netflix.titus.common.util.rx.ReactorExt;
import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.util.retry.Retry;

class GrpcClusterMembershipLeaderNameResolver extends NameResolver {

    private static final Logger logger = LoggerFactory.getLogger(GrpcClusterMembershipLeaderNameResolver.class);

    private static final String CLUSTER_MEMBERSHIP_AUTHORITY = "ClusterMembershipAuthority";

    private final GrpcClusterMembershipNameResolverConfiguration configuration;
    private final ClusterMemberResolver resolver;
    private final Function<ClusterMember, ClusterMemberAddress> addressSelector;

    private volatile Listener listener;
    private volatile Disposable eventStreamDisposable;
    private volatile ClusterMembershipRevision<ClusterMember> lastLeader;

    GrpcClusterMembershipLeaderNameResolver(GrpcClusterMembershipNameResolverConfiguration configuration,
                                            ClusterMemberResolver resolver,
                                            Function<ClusterMember, ClusterMemberAddress> addressSelector) {
        this.configuration = configuration;
        this.resolver = resolver;
        this.addressSelector = addressSelector;
    }

    @Override
    public String getServiceAuthority() {
        return CLUSTER_MEMBERSHIP_AUTHORITY;
    }

    @Override
    public void start(Listener listener) {
        this.listener = listener;

        Duration retryInterval = Duration.ofMillis(configuration.getRetryIntervalMs());
        this.eventStreamDisposable = resolver.resolve()
                .materialize()
                .flatMap(signal -> {
                    if (signal.getType() == SignalType.ON_NEXT) {
                        return Mono.just(signal.get());
                    }
                    if (signal.getType() == SignalType.ON_COMPLETE) {
                        return Mono.error(new IllegalStateException("Unexpected end of stream"));
                    }
                    if (signal.getType() == SignalType.ON_ERROR) {
                        return Mono.error(signal.getThrowable());
                    }
                    return Mono.empty();
                })
                .retryWhen(Retry.backoff(Long.MAX_VALUE, retryInterval))
                .subscribe(
                        this::refresh,
                        e -> logger.warn("Cluster membership event stream terminated with an error", e),
                        () -> logger.info("Cluster membership event stream terminated")
                );
    }

    @Override
    public void shutdown() {
        ReactorExt.safeDispose(eventStreamDisposable);
        this.listener = null;
    }

    private void refresh(ClusterMembershipSnapshot snapshot) {
        try {
            Optional<ClusterMembershipRevision<ClusterMember>> leaderOpt = snapshot.getLeaderRevision()
                    .flatMap(l -> Optional.ofNullable(snapshot.getMemberRevisions().get(l.getCurrent().getMemberId())));

            if (leaderOpt.isPresent()) {
                ClusterMembershipRevision<ClusterMember> memberRevision = leaderOpt.get();
                ClusterMemberAddress address = addressSelector.apply(memberRevision.getCurrent());

                if (lastLeader == null || !lastLeader.getCurrent().getMemberId().equals(memberRevision.getCurrent().getMemberId())) {
                    logger.info("New leader: {}", memberRevision);
                    lastLeader = memberRevision;
                } else {
                    logger.debug("Refreshing: {}", lastLeader);
                }

                EquivalentAddressGroup server = new EquivalentAddressGroup(new InetSocketAddress(address.getIpAddress(), address.getPortNumber()));
                List<EquivalentAddressGroup> servers = Collections.singletonList(server);
                listener.onAddresses(servers, Attributes.EMPTY);
            } else {
                if (lastLeader != null) {
                    lastLeader = null;
                    logger.warn("No leader");
                }
                listener.onError(Status.UNAVAILABLE.withDescription("Unable to resolve leader server"));
            }
        } catch (Exception e) {
            logger.error("Unable to create server with error: ", e);
            listener.onError(Status.UNAVAILABLE.withCause(e));
        }
    }
}
