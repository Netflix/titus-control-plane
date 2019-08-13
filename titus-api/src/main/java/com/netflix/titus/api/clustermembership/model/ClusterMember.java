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
    private final ClusterMemberLeadershipState leadershipState;
    private final List<ClusterMemberAddress> clusterMemberAddresses;
    private final Map<String, String> labels;

    public ClusterMember(String memberId,
                         ClusterMemberState state,
                         boolean enabled,
                         ClusterMemberLeadershipState leadershipState,
                         List<ClusterMemberAddress> clusterMemberAddresses,
                         Map<String, String> labels) {
        this.memberId = memberId;
        this.state = state;
        this.enabled = enabled;
        this.leadershipState = leadershipState;
        this.clusterMemberAddresses = clusterMemberAddresses;
        this.labels = labels;
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

    public ClusterMemberLeadershipState getLeadershipState() {
        return leadershipState;
    }

    public List<ClusterMemberAddress> getClusterMemberAddresses() {
        return clusterMemberAddresses;
    }

    public Map<String, String> getLabels() {
        return labels;
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
                leadershipState == that.leadershipState &&
                Objects.equals(clusterMemberAddresses, that.clusterMemberAddresses) &&
                Objects.equals(labels, that.labels);
    }

    @Override
    public int hashCode() {
        return Objects.hash(memberId, state, enabled, leadershipState, clusterMemberAddresses, labels);
    }

    @Override
    public String toString() {
        return "ClusterMember{" +
                "memberId='" + memberId + '\'' +
                ", state=" + state +
                ", enabled=" + enabled +
                ", leadershipState=" + leadershipState +
                ", clusterMemberAddresses=" + clusterMemberAddresses +
                ", labels=" + labels +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder().withMemberId(memberId).withState(state).withEnabled(true).withLeadershipState(leadershipState).withClusterMemberAddresses(clusterMemberAddresses).withLabels(labels);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String memberId;
        private ClusterMemberState state;
        private boolean enabled;
        private ClusterMemberLeadershipState leadershipState;
        private List<ClusterMemberAddress> clusterMemberAddress;
        private Map<String, String> labels;

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

        public Builder withLeadershipState(ClusterMemberLeadershipState leadershipState) {
            this.leadershipState = leadershipState;
            return this;
        }

        public Builder withClusterMemberAddresses(List<ClusterMemberAddress> clusterMemberAddress) {
            this.clusterMemberAddress = clusterMemberAddress;
            return this;
        }

        public Builder withLabels(Map<String, String> labels) {
            this.labels = labels;
            return this;
        }

        public ClusterMember build() {
            return new ClusterMember(memberId, state, enabled, leadershipState, clusterMemberAddress, labels);
        }
    }
}
