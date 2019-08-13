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

import java.util.Map;
import java.util.Objects;

public class ClusterMemberLeadership {

    private final String memberId;
    private final ClusterMemberLeadershipState leadershipState;
    private final Map<String, String> labels;

    public ClusterMemberLeadership(String memberId,
                                   ClusterMemberLeadershipState leadershipState,
                                   Map<String, String> labels) {
        this.memberId = memberId;
        this.leadershipState = leadershipState;
        this.labels = labels;
    }

    public String getMemberId() {
        return memberId;
    }

    public ClusterMemberLeadershipState getLeadershipState() {
        return leadershipState;
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
        ClusterMemberLeadership that = (ClusterMemberLeadership) o;
        return Objects.equals(memberId, that.memberId) &&
                leadershipState == that.leadershipState &&
                Objects.equals(labels, that.labels);
    }

    @Override
    public int hashCode() {
        return Objects.hash(memberId, leadershipState, labels);
    }

    @Override
    public String toString() {
        return "ClusterMemberLeadership{" +
                "memberId='" + memberId + '\'' +
                ", leadershipState=" + leadershipState +
                ", labels=" + labels +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder().withMemberId(memberId).withLeadershipState(leadershipState).withLabels(labels);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String memberId;
        private ClusterMemberLeadershipState leadershipState;
        private Map<String, String> labels;

        private Builder() {
        }

        public Builder withMemberId(String memberId) {
            this.memberId = memberId;
            return this;
        }

        public Builder withLeadershipState(ClusterMemberLeadershipState leadershipState) {
            this.leadershipState = leadershipState;
            return this;
        }

        public Builder withLabels(Map<String, String> labels) {
            this.labels = labels;
            return this;
        }

        public ClusterMemberLeadership build() {
            return new ClusterMemberLeadership(memberId, leadershipState, labels);
        }
    }
}
