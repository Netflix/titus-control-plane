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

package com.netflix.titus.runtime.clustermembership.endpoint.grpc;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadership;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadershipState;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipEvent;
import com.netflix.titus.api.clustermembership.service.ClusterMembershipService;
import com.netflix.titus.common.util.CollectionsExt;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static com.netflix.titus.testkit.model.clustermembership.ClusterMemberGenerator.activeClusterMember;
import static com.netflix.titus.testkit.model.clustermembership.ClusterMemberGenerator.clusterMemberRegistrationRevision;

public class ClusterMembershipServiceStub implements ClusterMembershipService {

    static final String LOCAL_MEMBER_ID = "local";

    static final ClusterMembershipRevision<ClusterMemberLeadership> INITIAL_LOCAL_LEADERSHIP =
            com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision.<ClusterMemberLeadership>newBuilder()
                    .withCurrent(ClusterMemberLeadership.newBuilder()
                            .withMemberId(LOCAL_MEMBER_ID)
                            .withLeadershipState(ClusterMemberLeadershipState.Leader)
                            .build()
                    )
                    .build();

    static final ClusterMembershipRevision<ClusterMember> LOCAL_MEMBER = clusterMemberRegistrationRevision(activeClusterMember(LOCAL_MEMBER_ID));

    static final ClusterMembershipRevision<ClusterMember> SIBLING_1 = clusterMemberRegistrationRevision(activeClusterMember("sibling1"));

    static final ClusterMembershipRevision<ClusterMember> SIBLING_2 = clusterMemberRegistrationRevision(activeClusterMember("sibling2"));

    private volatile ClusterMembershipRevision<ClusterMember> localMember;
    private volatile ClusterMembershipRevision<ClusterMemberLeadership> localLeadership;

    private volatile Map<String, ClusterMembershipRevision<ClusterMember>> siblings;
    private volatile ClusterMembershipRevision<ClusterMemberLeadership> siblingElection;

    private final DirectProcessor<ClusterMembershipEvent> eventProcessor = DirectProcessor.create();

    public ClusterMembershipServiceStub() {
        this.localMember = LOCAL_MEMBER;
        this.localLeadership = INITIAL_LOCAL_LEADERSHIP;

        this.siblings = new HashMap<>();
        siblings.put("sibling1", SIBLING_1);
        siblings.put("sibling2", SIBLING_2);
    }

    @Override
    public ClusterMembershipRevision<ClusterMember> getLocalClusterMember() {
        return localMember;
    }

    @Override
    public Map<String, ClusterMembershipRevision<ClusterMember>> getClusterMemberSiblings() {
        return siblings;
    }

    @Override
    public ClusterMembershipRevision<ClusterMemberLeadership> getLocalLeadership() {
        return localLeadership;
    }

    @Override
    public Optional<ClusterMembershipRevision<ClusterMemberLeadership>> findLeader() {
        if (localLeadership.getCurrent().getLeadershipState() == ClusterMemberLeadershipState.Leader) {
            return Optional.of(localLeadership);
        }
        if (siblingElection != null && siblingElection.getCurrent().getLeadershipState() == ClusterMemberLeadershipState.Leader) {
            return Optional.of(siblingElection);
        }
        return Optional.empty();
    }

    @Override
    public Mono<ClusterMembershipRevision<ClusterMember>> updateSelf(Function<ClusterMember, ClusterMembershipRevision<ClusterMember>> memberUpdate) {
        return Mono.fromCallable(() -> {
            this.localMember = memberUpdate.apply(localMember.getCurrent());
            eventProcessor.onNext(ClusterMembershipEvent.memberUpdatedEvent(localMember));
            return localMember;
        });
    }

    @Override
    public Mono<Void> stopBeingLeader() {
        return Mono.fromRunnable(() -> {
            if (localLeadership.getCurrent().getLeadershipState() != ClusterMemberLeadershipState.Leader) {
                return;
            }

            // Move local to non-leader
            this.localLeadership = ClusterMembershipRevision.<ClusterMemberLeadership>newBuilder()
                    .withCurrent(localLeadership.getCurrent().toBuilder().withLeadershipState(ClusterMemberLeadershipState.NonLeader).build())
                    .build();
            eventProcessor.onNext(ClusterMembershipEvent.leaderLost(localLeadership));

            // Elect new leader
            ClusterMembershipRevision<ClusterMember> sibling = CollectionsExt.first(siblings.values());
            this.siblingElection = ClusterMembershipRevision.<ClusterMemberLeadership>newBuilder()
                    .withCurrent(ClusterMemberLeadership.newBuilder()
                            .withMemberId(sibling.getCurrent().getMemberId())
                            .withLeadershipState(ClusterMemberLeadershipState.Leader)
                            .build()
                    )
                    .build();
            eventProcessor.onNext(ClusterMembershipEvent.leaderElected(siblingElection));
        });
    }

    @Override
    public Flux<ClusterMembershipEvent> events() {
        return Flux.<ClusterMembershipEvent>just(ClusterMembershipEvent.snapshotEvent(
                CollectionsExt.copyAndAddToList(siblings.values(), localMember),
                localLeadership,
                Optional.of(localLeadership)
        )).concatWith(eventProcessor);
    }
}
