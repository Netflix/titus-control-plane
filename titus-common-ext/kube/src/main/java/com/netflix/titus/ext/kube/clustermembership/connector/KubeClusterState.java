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

package com.netflix.titus.ext.kube.clustermembership.connector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadership;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadershipState;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipChangeEvent;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipEvent;
import com.netflix.titus.api.clustermembership.model.event.LeaderElectionChangeEvent;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.time.Clock;

public class KubeClusterState {

    private final String localMemberId;

    private final ClusterMembershipRevision<ClusterMember> localMemberRevision;
    private final ClusterMembershipRevision<ClusterMemberLeadership> localMemberLeadershipRevision;

    private final boolean inLeaderElectionProcess;
    private final boolean localLeader;

    private final Map<String, ClusterMembershipRevision<ClusterMember>> clusterMemberSiblings;
    private final Optional<ClusterMembershipRevision<ClusterMemberLeadership>> currentLeaderOptional;

    private final List<ClusterMembershipEvent> events;

    private final KubeClusterMembershipConfiguration configuration;
    private final Clock clock;

    public KubeClusterState(ClusterMember initial,
                            KubeClusterMembershipConfiguration configuration,
                            Clock clock) {
        this.configuration = configuration;
        this.clock = clock;
        this.localMemberId = initial.getMemberId();
        this.localMemberRevision = ClusterMembershipRevision.<ClusterMember>newBuilder()
                .withCurrent(initial)
                .withRevision(-1)
                .withCode("initial")
                .withMessage("Initial")
                .withTimestamp(clock.wallTime())
                .build();
        this.localMemberLeadershipRevision = ClusterMembershipRevision.<ClusterMemberLeadership>newBuilder()
                .withCurrent(ClusterMemberLeadership.newBuilder()
                        .withMemberId(initial.getMemberId())
                        .withLeadershipState(ClusterMemberLeadershipState.Disabled)
                        .build()
                )
                .withRevision(-1)
                .withCode("initial")
                .withMessage("Initial")
                .withTimestamp(clock.wallTime())
                .build();
        this.inLeaderElectionProcess = false;
        this.localLeader = false;
        this.clusterMemberSiblings = Collections.emptyMap();
        this.currentLeaderOptional = Optional.empty();
        this.events = Collections.emptyList();
    }

    private KubeClusterState(ClusterMembershipRevision<ClusterMember> localMemberRevision,
                             ClusterMembershipRevision<ClusterMemberLeadership> localMemberLeadershipRevision,
                             boolean inLeaderElectionProcess,
                             boolean localLeader,
                             Map<String, ClusterMembershipRevision<ClusterMember>> clusterMemberSiblings,
                             Optional<ClusterMembershipRevision<ClusterMemberLeadership>> currentLeaderOptional,
                             List<ClusterMembershipEvent> events,
                             KubeClusterMembershipConfiguration configuration,
                             Clock clock) {
        this.localMemberId = localMemberLeadershipRevision.getCurrent().getMemberId();
        this.localMemberRevision = localMemberRevision;
        this.localMemberLeadershipRevision = localMemberLeadershipRevision;
        this.inLeaderElectionProcess = inLeaderElectionProcess;
        this.localLeader = localLeader;
        this.clusterMemberSiblings = clusterMemberSiblings;
        this.currentLeaderOptional = currentLeaderOptional;
        this.events = events;
        this.configuration = configuration;
        this.clock = clock;
    }

    public boolean isRegistered() {
        return localMemberRevision.getCurrent().isRegistered();
    }

    public boolean isInLeaderElectionProcess() {
        return inLeaderElectionProcess;
    }

    public boolean isLocalLeader() {
        return localLeader;
    }

    public ClusterMembershipRevision<ClusterMember> getLocalMemberRevision() {
        return localMemberRevision;
    }

    public ClusterMembershipRevision<ClusterMemberLeadership> getLocalMemberLeadershipRevision() {
        return localMemberLeadershipRevision;
    }

    public Map<String, ClusterMembershipRevision<ClusterMember>> getStaleClusterMemberSiblings() {
        return CollectionsExt.copyAndRemoveByValue(clusterMemberSiblings, r -> !isStale(r));
    }

    public Map<String, ClusterMembershipRevision<ClusterMember>> getNotStaleClusterMemberSiblings() {
        return CollectionsExt.copyAndRemoveByValue(clusterMemberSiblings, this::isStale);
    }

    public Optional<ClusterMembershipRevision<ClusterMemberLeadership>> findCurrentLeader() {
        return currentLeaderOptional;
    }

    public ClusterMembershipEvent getSnapshotEvent() {
        return ClusterMembershipEvent.snapshotEvent(
                CollectionsExt.copyAndAddToList(getNotStaleClusterMemberSiblings().values(), localMemberRevision),
                localMemberLeadershipRevision,
                currentLeaderOptional
        );
    }

    /**
     * Events describing changes since the previous version of the object.
     */
    public List<ClusterMembershipEvent> getDeltaEvents() {
        return events;
    }

    public KubeClusterState setLocalClusterMemberRevision(ClusterMembershipRevision<ClusterMember> localMemberRevision) {
        return toBuilder()
                .withLocalMemberRevision(localMemberRevision)
                .withEvent(ClusterMembershipEvent.memberUpdatedEvent(localMemberRevision))
                .build();
    }

    public KubeClusterState setJoinedLeaderElection() {
        ClusterMembershipRevision<ClusterMemberLeadership> revision = ClusterMembershipRevision.<ClusterMemberLeadership>newBuilder()
                .withCurrent(ClusterMemberLeadership.newBuilder()
                        .withMemberId(localMemberRevision.getCurrent().getMemberId())
                        .withLeadershipState(ClusterMemberLeadershipState.NonLeader)
                        .build()
                )
                .build();
        return toBuilder()
                .withInLeaderElectionProcess(true)
                .withLocalMemberLeadershipRevision(revision)
                .withEvent(LeaderElectionChangeEvent.localJoinedElection(revision))
                .build();
    }

    public KubeClusterState setLeaveLeaderElection() {
        ClusterMembershipRevision<ClusterMemberLeadership> revision = ClusterMembershipRevision.<ClusterMemberLeadership>newBuilder()
                .withCurrent(ClusterMemberLeadership.newBuilder()
                        .withMemberId(localMemberId)
                        .withLeadershipState(ClusterMemberLeadershipState.Disabled)
                        .build()
                )
                .build();

        LeaderElectionChangeEvent event = localLeader
                ? LeaderElectionChangeEvent.leaderLost(revision)
                : LeaderElectionChangeEvent.localLeftElection(revision);

        return toBuilder()
                .withInLeaderElectionProcess(false)
                .withLocalMemberLeadershipRevision(revision)
                .withEvent(event)
                .build();
    }

    public KubeClusterState processMembershipEventStreamEvent(ClusterMembershipChangeEvent event) {
        String eventMemberId = event.getRevision().getCurrent().getMemberId();

        // We emit local events at the time updates are made. We assume here that each member modifies its data only.
        // If somebody else tampers with the data, the re-registration process will fix it.
        if (localMemberId.equals(eventMemberId)) {
            return this;
        }

        switch (event.getChangeType()) {
            case Added:
            case Updated:
                Builder builder = toBuilder()
                        .withClusterMemberSiblings(CollectionsExt.copyAndAdd(clusterMemberSiblings, eventMemberId, event.getRevision()));

                // Filter out stale members not seen before
                if (clusterMemberSiblings.containsKey(eventMemberId) || !isStale(event.getRevision())) {
                    builder.withEvent(event);
                }

                return builder.build();
            case Removed:
                if (!clusterMemberSiblings.containsKey(eventMemberId)) {
                    return this;
                }
                return toBuilder()
                        .withClusterMemberSiblings(CollectionsExt.copyAndRemove(clusterMemberSiblings, eventMemberId))
                        .withEvent(event)
                        .build();
        }
        return this;
    }

    public KubeClusterState processLeaderElectionEventStreamEvent(LeaderElectionChangeEvent event) {
        String eventMemberId = event.getLeadershipRevision().getCurrent().getMemberId();
        boolean local = localMemberId.equals(eventMemberId);

        switch (event.getChangeType()) {
            case LeaderElected:
                Builder electedBuilder = toBuilder()
                        .withLocalLeader(local)
                        .withCurrentLeader(event.getLeadershipRevision())
                        .withEvent(event);
                if (local) {
                    electedBuilder.withLocalMemberLeadershipRevision(event.getLeadershipRevision());
                }
                return electedBuilder.build();
            case LeaderLost:
                Builder lostBuilder = toBuilder()
                        .withLocalLeader(false)
                        .withCurrentLeader(null)
                        .withEvent(event);
                if (local) {
                    lostBuilder.withLocalMemberLeadershipRevision(event.getLeadershipRevision());
                }
                return lostBuilder.build();
        }
        return this;
    }

    public KubeClusterState removeStaleMember(String memberId) {
        ClusterMembershipRevision<ClusterMember> staleMember = clusterMemberSiblings.get(memberId);
        if (staleMember == null) {
            return this;
        }
        return toBuilder()
                .withClusterMemberSiblings(CollectionsExt.copyAndRemove(clusterMemberSiblings, memberId))
                .withEvent(ClusterMembershipChangeEvent.memberRemovedEvent(staleMember))
                .build();
    }

    private boolean isStale(ClusterMembershipRevision<ClusterMember> memberRevision) {
        return clock.isPast(memberRevision.getTimestamp() + configuration.getRegistrationStaleThresholdMs());
    }

    private Builder toBuilder() {
        return new Builder()
                .withLocalMemberRevision(localMemberRevision)
                .withLocalMemberLeadershipRevision(localMemberLeadershipRevision)
                .withInLeaderElectionProcess(inLeaderElectionProcess)
                .withLocalLeader(localLeader)
                .withClusterMemberSiblings(clusterMemberSiblings)
                .withCurrentLeader(currentLeaderOptional.orElse(null))
                .withConfiguration(configuration)
                .withClock(clock);
    }

    static final class Builder {

        private ClusterMembershipRevision<ClusterMember> localMemberRevision;
        private ClusterMembershipRevision<ClusterMemberLeadership> localMemberLeadershipRevision;
        private boolean inLeaderElectionProcess;
        private boolean localLeader;
        private Map<String, ClusterMembershipRevision<ClusterMember>> clusterMemberSiblings;
        private Optional<ClusterMembershipRevision<ClusterMemberLeadership>> currentLeaderOptional;
        private final List<ClusterMembershipEvent> events = new ArrayList<>();

        private KubeClusterMembershipConfiguration configuration;
        private Clock clock;

        private Builder() {
        }

        Builder withLocalMemberRevision(ClusterMembershipRevision<ClusterMember> localMemberRevision) {
            this.localMemberRevision = localMemberRevision;
            return this;
        }

        Builder withLocalMemberLeadershipRevision(ClusterMembershipRevision<ClusterMemberLeadership> localMemberLeadershipRevision) {
            this.localMemberLeadershipRevision = localMemberLeadershipRevision;
            return this;
        }

        Builder withInLeaderElectionProcess(boolean inLeaderElectionProcess) {
            this.inLeaderElectionProcess = inLeaderElectionProcess;
            return this;
        }

        Builder withLocalLeader(boolean localLeader) {
            this.localLeader = localLeader;
            return this;
        }

        Builder withClusterMemberSiblings(Map<String, ClusterMembershipRevision<ClusterMember>> clusterMemberSiblings) {
            this.clusterMemberSiblings = clusterMemberSiblings;
            return this;
        }

        Builder withCurrentLeader(ClusterMembershipRevision<ClusterMemberLeadership> currentLeader) {
            this.currentLeaderOptional = Optional.ofNullable(currentLeader);
            return this;
        }

        Builder withEvent(ClusterMembershipEvent event) {
            events.add(event);
            return this;
        }

        Builder withConfiguration(KubeClusterMembershipConfiguration configuration) {
            this.configuration = configuration;
            return this;
        }

        Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        KubeClusterState build() {
            return new KubeClusterState(
                    localMemberRevision,
                    localMemberLeadershipRevision,
                    inLeaderElectionProcess,
                    localLeader,
                    clusterMemberSiblings,
                    currentLeaderOptional,
                    events,
                    configuration,
                    clock
            );
        }
    }
}
