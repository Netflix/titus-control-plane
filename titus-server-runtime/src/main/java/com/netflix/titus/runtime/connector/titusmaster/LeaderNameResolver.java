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

package com.netflix.titus.runtime.connector.titusmaster;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.rx.ObservableExt;
import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscription;

public class LeaderNameResolver extends NameResolver {
    private static final Logger logger = LoggerFactory.getLogger(LeaderNameResolver.class);

    private final String authority;
    private final LeaderResolver leaderResolver;
    private final int port;
    private final TitusRuntime titusRuntime;

    @GuardedBy("this")
    private boolean shutdown;
    @GuardedBy("this")
    private Listener listener;
    @GuardedBy("this")
    private Subscription leaderSubscription;

    public LeaderNameResolver(URI targetUri,
                              LeaderResolver leaderResolver,
                              int port,
                              TitusRuntime titusRuntime) {

        this.leaderResolver = leaderResolver;
        this.port = port;
        this.titusRuntime = titusRuntime;

        if (targetUri.getAuthority() != null) {
            this.authority = targetUri.getAuthority();
        } else {
            this.authority = targetUri.getPath().substring(1);
        }
    }

    @Override
    public String getServiceAuthority() {
        return this.authority;
    }

    @Override
    public void start(Listener listener) {
        Preconditions.checkState(this.listener == null, "already started");
        this.listener = Preconditions.checkNotNull(listener, "listener");

        try {
            Optional<Address> leaderAddressOpt = leaderResolver.resolve();
            refreshServers(listener, leaderAddressOpt);
        } catch (Exception e) {
            logger.error("Unable to resolve leader on start with error: ", e);
            listener.onError(Status.UNAVAILABLE.withCause(e));
        }

        leaderSubscription = titusRuntime.persistentStream(leaderResolver.observeLeader()).subscribe(
                leaderAddressOpt -> refreshServers(listener, leaderAddressOpt),
                e -> {
                    logger.error("Unable to observe leader with error: ", e);
                    listener.onError(Status.UNAVAILABLE.withCause(e));
                },
                () -> logger.debug("Completed the leader resolver observable")
        );
    }

    private void refreshServers(Listener listener, Optional<Address> leaderAddressOpt) {
        List<EquivalentAddressGroup> servers = new ArrayList<>();
        try {
            if (leaderAddressOpt.isPresent()) {
                Address address = leaderAddressOpt.get();
                EquivalentAddressGroup server = new EquivalentAddressGroup(new InetSocketAddress(address.getHost(), port));
                servers.add(server);
            }
        } catch (Exception e) {
            logger.error("Unable to create server with error: ", e);
            listener.onError(Status.UNAVAILABLE.withCause(e));
        }

        if (servers.isEmpty()) {
            listener.onError(Status.UNAVAILABLE.withDescription("Unable to resolve leader server. Refreshing."));
        } else {
            logger.debug("Refreshing servers: {}", servers);
            listener.onAddresses(servers, Attributes.EMPTY);
        }
    }

    public final synchronized void shutdown() {
        if (!this.shutdown) {
            this.shutdown = true;
            ObservableExt.safeUnsubscribe(leaderSubscription);
        }
    }
}