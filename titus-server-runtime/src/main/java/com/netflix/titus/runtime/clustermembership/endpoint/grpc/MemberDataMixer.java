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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadership;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipChangeEvent;
import com.netflix.titus.api.clustermembership.model.event.LeaderElectionChangeEvent;
import com.netflix.titus.grpc.protogen.ClusterMembershipEvent;

import static com.netflix.titus.runtime.clustermembership.endpoint.grpc.ClusterMembershipGrpcConverters.toGrpcClusterMembershipRevision;

/**
 * At the connector and service level we keep membership data and the leader election process separate. However
 * when exposing the data outside, we want to merge this into single data model. This class deals with the complexity
 * of doing that.
 */
class MemberDataMixer {

    static final String NO_LEADER_ID = "";

    private final Map<String, ClusterMembershipRevision<ClusterMember>> memberRevisions;
    private String leaderId;

    MemberDataMixer(List<ClusterMembershipRevision<ClusterMember>> memberRevisions,
                    Optional<ClusterMembershipRevision<ClusterMemberLeadership>> currentLeaderOptional) {
        this.memberRevisions = memberRevisions.stream().collect(Collectors.toMap(r -> r.getCurrent().getMemberId(), r -> r));
        this.leaderId = currentLeaderOptional.map(l -> l.getCurrent().getMemberId()).orElse(NO_LEADER_ID);
    }

    List<ClusterMembershipEvent> process(com.netflix.titus.api.clustermembership.model.event.ClusterMembershipEvent coreEvent) {
        if (coreEvent instanceof ClusterMembershipChangeEvent) {
            return processMembershipChangeEvent((ClusterMembershipChangeEvent) coreEvent);
        }
        if (coreEvent instanceof LeaderElectionChangeEvent) {
            return processLeaderElectionChangeEvent((LeaderElectionChangeEvent) coreEvent);
        }
        return Collections.emptyList();
    }

    private List<ClusterMembershipEvent> processMembershipChangeEvent(ClusterMembershipChangeEvent coreEvent) {
        ClusterMembershipRevision<ClusterMember> revision = coreEvent.getRevision();
        String memberId = revision.getCurrent().getMemberId();

        if (coreEvent.getChangeType() == ClusterMembershipChangeEvent.ChangeType.Removed) {
            memberRevisions.remove(memberId);
            if (isLeader(revision)) {
                leaderId = NO_LEADER_ID;
            }

            return Collections.singletonList(ClusterMembershipEvent.newBuilder()
                    .setMemberRemoved(ClusterMembershipEvent.MemberRemoved.newBuilder()
                            .setRevision(toGrpcClusterMembershipRevision(revision, false))
                    )
                    .build()
            );
        }

        memberRevisions.put(memberId, revision);
        return Collections.singletonList(newMemberUpdatedEvent(revision, isLeader(revision)));
    }

    private List<ClusterMembershipEvent> processLeaderElectionChangeEvent(LeaderElectionChangeEvent coreEvent) {
        ClusterMembershipRevision<ClusterMemberLeadership> revision = coreEvent.getLeadershipRevision();
        String memberId = revision.getCurrent().getMemberId();

        if (coreEvent.getChangeType() == LeaderElectionChangeEvent.ChangeType.LocalJoined) {
            return Collections.emptyList();
        }

        if (coreEvent.getChangeType() == LeaderElectionChangeEvent.ChangeType.LocalLeft) {
            return Collections.emptyList();
        }

        if (coreEvent.getChangeType() == LeaderElectionChangeEvent.ChangeType.LeaderElected) {
            if (this.leaderId.equals(memberId)) {
                return Collections.emptyList();
            }
            ClusterMembershipRevision<ClusterMember> previousLeader = memberRevisions.get(this.leaderId);
            ClusterMembershipRevision<ClusterMember> currentLeader = memberRevisions.get(memberId);

            List<ClusterMembershipEvent> events = new ArrayList<>();

            if (previousLeader != null) {
                events.add(newMemberUpdatedEvent(previousLeader, false));
            }
            if (currentLeader != null) {
                events.add(newMemberUpdatedEvent(currentLeader, true));
            }

            this.leaderId = revision.getCurrent().getMemberId();
            return events;
        }

        if (coreEvent.getChangeType() == LeaderElectionChangeEvent.ChangeType.LeaderLost) {
            if (leaderId.equals(NO_LEADER_ID)) {
                return Collections.emptyList();
            }
            ClusterMembershipRevision<ClusterMember> lostLeader = memberRevisions.get(leaderId);
            if (lostLeader == null) {
                return Collections.emptyList();
            }
            this.leaderId = NO_LEADER_ID;
            return Collections.singletonList(newMemberUpdatedEvent(lostLeader, false));
        }

        return Collections.emptyList();
    }

    ClusterMembershipEvent toGrpcSnapshot() {
        ClusterMembershipEvent.Snapshot.Builder builder = ClusterMembershipEvent.Snapshot.newBuilder();
        memberRevisions.forEach((id, revision) -> builder.addRevisions(toGrpcClusterMembershipRevision(revision, isLeader(revision))));
        return ClusterMembershipEvent.newBuilder()
                .setSnapshot(builder)
                .build();
    }

    private boolean isLeader(ClusterMembershipRevision<ClusterMember> revision) {
        return revision.getCurrent().getMemberId().equals(leaderId);
    }

    private ClusterMembershipEvent newMemberUpdatedEvent(ClusterMembershipRevision<ClusterMember> revision, boolean leader) {
        return ClusterMembershipEvent.newBuilder()
                .setMemberUpdated(ClusterMembershipEvent.MemberUpdated.newBuilder()
                        .setRevision(toGrpcClusterMembershipRevision(revision, leader))
                )
                .build();
    }
}
