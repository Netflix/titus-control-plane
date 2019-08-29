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

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import com.netflix.titus.api.clustermembership.connector.ClusterMembershipConnector;
import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadership;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadershipState;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipEvent;
import com.netflix.titus.testkit.model.clustermembership.ClusterMemberGenerator;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class ClusterMembershipConnectorStub implements ClusterMembershipConnector {

    private static final String LOCAL_MEMBER_ID = "localMember";

    private volatile ClusterMembershipRevision<ClusterMember> localMemberRevision;
    private volatile ClusterMembershipRevision<ClusterMemberLeadership> localLeadershipRevision;

    private final DirectProcessor<ClusterMembershipEvent> eventProcessor = DirectProcessor.create();

    ClusterMembershipConnectorStub() {
        this.localMemberRevision = ClusterMembershipRevision.<ClusterMember>newBuilder()
                .withCurrent(ClusterMemberGenerator.activeClusterMember(LOCAL_MEMBER_ID).toBuilder()
                        .withRegistered(true)
                        .build()
                )
                .build();
        this.localLeadershipRevision = ClusterMembershipRevision.<ClusterMemberLeadership>newBuilder()
                .withCurrent(ClusterMemberLeadership.newBuilder()
                        .withMemberId(LOCAL_MEMBER_ID)
                        .withLeadershipState(ClusterMemberLeadershipState.Disabled)
                        .build()
                )
                .build();
    }

    @Override
    public ClusterMembershipRevision<ClusterMember> getLocalClusterMemberRevision() {
        return localMemberRevision;
    }

    @Override
    public Map<String, ClusterMembershipRevision<ClusterMember>> getClusterMemberSiblings() {
        throw new IllegalStateException("not implemented yet");
    }

    @Override
    public ClusterMembershipRevision<ClusterMemberLeadership> getLocalLeadershipRevision() {
        return localLeadershipRevision;
    }

    @Override
    public Optional<ClusterMembershipRevision<ClusterMemberLeadership>> findCurrentLeader() {
        return localLeadershipRevision.getCurrent().getLeadershipState() == ClusterMemberLeadershipState.Leader
                ? Optional.of(localLeadershipRevision)
                : Optional.empty();
    }

    @Override
    public Mono<ClusterMembershipRevision<ClusterMember>> register(Function<ClusterMember, ClusterMembershipRevision<ClusterMember>> selfUpdate) {
        return Mono.fromCallable(() -> {
            this.localMemberRevision = selfUpdate.apply(localMemberRevision.getCurrent());
            eventProcessor.onNext(ClusterMembershipEvent.memberUpdatedEvent(localMemberRevision));
            return localMemberRevision;
        });
    }

    @Override
    public Mono<ClusterMembershipRevision<ClusterMember>> unregister(Function<ClusterMember, ClusterMembershipRevision<ClusterMember>> selfUpdate) {
        throw new IllegalStateException("not implemented yet");
    }

    @Override
    public Mono<Void> joinLeadershipGroup() {
        return Mono.defer(() -> {
            ClusterMemberLeadershipState state = localLeadershipRevision.getCurrent().getLeadershipState();
            if (state == ClusterMemberLeadershipState.Disabled) {
                this.localLeadershipRevision = newLocalLeadershipState(ClusterMemberLeadershipState.NonLeader);
            }
            eventProcessor.onNext(ClusterMembershipEvent.localJoinedElection(localLeadershipRevision));
            return Mono.empty();
        });
    }

    @Override
    public Mono<Boolean> leaveLeadershipGroup(boolean onlyNonLeader) {
        return Mono.defer(() -> {
            ClusterMemberLeadershipState state = localLeadershipRevision.getCurrent().getLeadershipState();
            if (state == ClusterMemberLeadershipState.Disabled) {
                return Mono.just(true);
            }
            if (onlyNonLeader && state == ClusterMemberLeadershipState.Leader) {
                return Mono.just(false);
            }
            this.localLeadershipRevision = newLocalLeadershipState(ClusterMemberLeadershipState.Disabled);
            eventProcessor.onNext(ClusterMembershipEvent.localLeftElection(localLeadershipRevision));
            return Mono.just(true);
        });
    }

    @Override
    public Flux<ClusterMembershipEvent> membershipChangeEvents() {
        return Flux.defer(() -> Flux.<ClusterMembershipEvent>
                just(ClusterMembershipEvent.snapshotEvent(Collections.emptyList(), localLeadershipRevision, Optional.empty()))
                .concatWith(eventProcessor)
        );
    }

    void becomeLeader() {
        this.localLeadershipRevision = newLocalLeadershipState(ClusterMemberLeadershipState.Leader);
        eventProcessor.onNext(ClusterMembershipEvent.localJoinedElection(localLeadershipRevision));
    }

    private ClusterMembershipRevision<ClusterMemberLeadership> newLocalLeadershipState(ClusterMemberLeadershipState state) {
        return localLeadershipRevision.toBuilder()
                .withCurrent(localLeadershipRevision.getCurrent().toBuilder()
                        .withLeadershipState(state)
                        .build()
                )
                .build();
    }
}
