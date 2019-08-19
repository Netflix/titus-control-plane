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

package com.netflix.titus.api.clustermembership.model.event;

import java.util.List;

import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadership;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipChangeEvent.ChangeType;

public abstract class ClusterMembershipEvent {

    public static ClusterMembershipSnapshotEvent snapshotEvent(
            List<ClusterMembershipRevision<ClusterMember>> clusterMemberRevisions,
            ClusterMembershipRevision<ClusterMemberLeadership> localLeadership) {
        return new ClusterMembershipSnapshotEvent(clusterMemberRevisions, localLeadership);
    }

    public static ClusterMembershipChangeEvent memberAddedEvent(ClusterMembershipRevision<ClusterMember> revision) {
        return new ClusterMembershipChangeEvent(ChangeType.Added, revision);
    }

    public static ClusterMembershipChangeEvent memberRemovedEvent(ClusterMembershipRevision<ClusterMember> revision) {
        return new ClusterMembershipChangeEvent(ChangeType.Removed, revision);
    }

    public static ClusterMembershipChangeEvent memberUpdatedEvent(ClusterMembershipRevision<ClusterMember> revision) {
        return new ClusterMembershipChangeEvent(ChangeType.Updated, revision);
    }

    public static ClusterMembershipEvent disconnectedEvent(Throwable cause) {
        return new ClusterMembershipDisconnectedEvent(cause);
    }

    public static LeaderElectionChangeEvent localJoinedElection(ClusterMembershipRevision<ClusterMemberLeadership> leadershipRevision) {
        return new LeaderElectionChangeEvent(LeaderElectionChangeEvent.ChangeType.LocalJoined, leadershipRevision);
    }

    public static LeaderElectionChangeEvent localLeftElection(ClusterMembershipRevision<ClusterMemberLeadership> leadershipRevision) {
        return new LeaderElectionChangeEvent(LeaderElectionChangeEvent.ChangeType.LocalLeft, leadershipRevision);
    }

    public static LeaderElectionChangeEvent leaderLost(ClusterMembershipRevision<ClusterMemberLeadership> leadershipRevision) {
        return new LeaderElectionChangeEvent(LeaderElectionChangeEvent.ChangeType.LeaderLost, leadershipRevision);
    }

    public static LeaderElectionChangeEvent leaderElected(ClusterMembershipRevision<ClusterMemberLeadership> leadershipRevision) {
        return new LeaderElectionChangeEvent(LeaderElectionChangeEvent.ChangeType.LeaderElected, leadershipRevision);
    }
}
