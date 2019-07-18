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

import java.util.Objects;

import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadership;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;

public class LeaderElectionChangeEvent extends ClusterMembershipEvent {

    public enum ChangeType {
        Leader,
        LostLeadership
    }

    private final ChangeType changeType;
    private final ClusterMembershipRevision<ClusterMemberLeadership> leadershipRevision;

    LeaderElectionChangeEvent(ChangeType changeType, ClusterMembershipRevision<ClusterMemberLeadership> leadershipRevision) {
        this.changeType = changeType;
        this.leadershipRevision = leadershipRevision;
    }

    public ChangeType getChangeType() {
        return changeType;
    }

    public ClusterMembershipRevision<ClusterMemberLeadership> getLeadershipRevision() {
        return leadershipRevision;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LeaderElectionChangeEvent that = (LeaderElectionChangeEvent) o;
        return changeType == that.changeType &&
                Objects.equals(leadershipRevision, that.leadershipRevision);
    }

    @Override
    public int hashCode() {
        return Objects.hash(changeType, leadershipRevision);
    }

    @Override
    public String toString() {
        return "LeaderElectionChangeEvent{" +
                "changeType=" + changeType +
                ", leadershipRevision=" + leadershipRevision +
                '}';
    }
}
