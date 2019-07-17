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

package com.netflix.titus.api.clustermembership.model;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ClusterMember {

    private final String memberId;
    private final ClusterMemberState state;
    private final boolean enabled;
    private final ClusterMemberLeadershipState leadershipStatus;
    private final List<ClusterMemberAddress> clusterMemberAddress;
    private final Map<String, String> labels;
    private final List<ClusterMembershipRevision> revisions;

    public ClusterMember(String memberId,
                         ClusterMemberState state,
                         boolean enabled,
                         ClusterMemberLeadershipState leadershipStatus,
                         List<ClusterMemberAddress> clusterMemberAddress,
                         Map<String, String> labels,
                         List<ClusterMembershipRevision> revisions) {
        this.memberId = memberId;
        this.state = state;
        this.enabled = enabled;
        this.leadershipStatus = leadershipStatus;
        this.clusterMemberAddress = clusterMemberAddress;
        this.labels = labels;
        this.revisions = revisions;
    }

    public String getMemberId() {
        return memberId;
    }

    public ClusterMemberState getState() {
        return state;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public ClusterMemberLeadershipState getLeadershipStatus() {
        return leadershipStatus;
    }

    public List<ClusterMemberAddress> getClusterMemberAddress() {
        return clusterMemberAddress;
    }

    public Map<String, String> getLabels() {
        return labels;
    }

    public List<ClusterMembershipRevision> getRevisions() {
        return revisions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClusterMember that = (ClusterMember) o;
        return enabled == that.enabled &&
                Objects.equals(memberId, that.memberId) &&
                state == that.state &&
                leadershipStatus == that.leadershipStatus &&
                Objects.equals(clusterMemberAddress, that.clusterMemberAddress) &&
                Objects.equals(labels, that.labels) &&
                Objects.equals(revisions, that.revisions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(memberId, state, enabled, leadershipStatus, clusterMemberAddress, labels, revisions);
    }

    @Override
    public String toString() {
        return "ClusterMember{" +
                "memberId='" + memberId + '\'' +
                ", state=" + state +
                ", enabled=" + enabled +
                ", leadershipStatus=" + leadershipStatus +
                ", clusterMemberAddress=" + clusterMemberAddress +
                ", labels=" + labels +
                ", revisions=" + revisions +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder().withMemberId(memberId).withState(state).withEnabled(true).withLeadershipStatus(leadershipStatus).withClusterMemberAddress(clusterMemberAddress).withLabels(labels).withRevisions(revisions);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String memberId;
        private ClusterMemberState state;
        private boolean enabled;
        private ClusterMemberLeadershipState leadershipStatus;
        private List<ClusterMemberAddress> clusterMemberAddress;
        private Map<String, String> labels;
        private List<ClusterMembershipRevision> revisions;

        private Builder() {
        }

        public Builder withMemberId(String memberId) {
            this.memberId = memberId;
            return this;
        }

        public Builder withState(ClusterMemberState state) {
            this.state = state;
            return this;
        }

        public Builder withEnabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder withLeadershipStatus(ClusterMemberLeadershipState leadershipStatus) {
            this.leadershipStatus = leadershipStatus;
            return this;
        }

        public Builder withClusterMemberAddress(List<ClusterMemberAddress> clusterMemberAddress) {
            this.clusterMemberAddress = clusterMemberAddress;
            return this;
        }

        public Builder withLabels(Map<String, String> labels) {
            this.labels = labels;
            return this;
        }

        public Builder withRevisions(List<ClusterMembershipRevision> revisions) {
            this.revisions = revisions;
            return this;
        }

        public ClusterMember build() {
            return new ClusterMember(memberId, state, enabled, leadershipStatus, clusterMemberAddress, labels, revisions);
        }
    }
}
