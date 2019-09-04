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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.netflix.titus.common.util.CollectionsExt;

public class ClusterMembershipSnapshot {

    private static final ClusterMembershipSnapshot EMPTY = new ClusterMembershipSnapshot(Collections.emptyMap(), Optional.empty(), -1);

    private final Map<String, ClusterMembershipRevision<ClusterMember>> memberRevisions;
    private final Optional<ClusterMembershipRevision<ClusterMemberLeadership>> leaderRevision;
    private final long stalenessMs;

    public ClusterMembershipSnapshot(Map<String, ClusterMembershipRevision<ClusterMember>> memberRevisions,
                                     Optional<ClusterMembershipRevision<ClusterMemberLeadership>> leaderRevision,
                                     long stalenessMs) {
        this.memberRevisions = CollectionsExt.nonNull(memberRevisions);
        this.leaderRevision = leaderRevision == null ? Optional.empty() : leaderRevision;
        this.stalenessMs = stalenessMs;
    }

    public Map<String, ClusterMembershipRevision<ClusterMember>> getMemberRevisions() {
        return memberRevisions;
    }

    public Optional<ClusterMembershipRevision<ClusterMemberLeadership>> getLeaderRevision() {
        return leaderRevision;
    }

    public long getStalenessMs() {
        return stalenessMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClusterMembershipSnapshot snapshot = (ClusterMembershipSnapshot) o;
        return stalenessMs == snapshot.stalenessMs &&
                Objects.equals(memberRevisions, snapshot.memberRevisions) &&
                Objects.equals(leaderRevision, snapshot.leaderRevision);
    }

    @Override
    public int hashCode() {
        return Objects.hash(memberRevisions, leaderRevision, stalenessMs);
    }

    @Override
    public String toString() {
        return "ClusterMembershipSnapshot{" +
                "memberRevisions=" + memberRevisions +
                ", leaderRevision=" + leaderRevision +
                ", stalenessMs=" + stalenessMs +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder().withMemberRevisions(memberRevisions).withLeaderRevision(leaderRevision.orElse(null)).withStalenessMs(stalenessMs);
    }

    public static ClusterMembershipSnapshot empty() {
        return EMPTY;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {

        private Map<String, ClusterMembershipRevision<ClusterMember>> memberRevisions = new HashMap<>();
        private ClusterMembershipRevision<ClusterMemberLeadership> leaderRevision;
        private long stalenessMs;

        private Builder() {
        }

        @SafeVarargs
        public final Builder withMemberRevisions(ClusterMembershipRevision<ClusterMember>... revisions) {
            for (ClusterMembershipRevision<ClusterMember> revision : revisions) {
                memberRevisions.put(revision.getCurrent().getMemberId(), revision);
            }
            return this;
        }

        public final Builder withMemberRevisions(Collection<ClusterMembershipRevision<ClusterMember>> revisions) {
            for (ClusterMembershipRevision<ClusterMember> revision : revisions) {
                memberRevisions.put(revision.getCurrent().getMemberId(), revision);
            }
            return this;
        }

        public Builder withMemberRevisions(Map<String, ClusterMembershipRevision<ClusterMember>> memberRevisions) {
            this.memberRevisions.clear();
            this.memberRevisions.putAll(memberRevisions);
            return this;
        }

        @SafeVarargs
        public final Builder withoutMemberRevisions(ClusterMembershipRevision<ClusterMember>... revisions) {
            for (ClusterMembershipRevision<ClusterMember> revision : revisions) {
                memberRevisions.remove(revision.getCurrent().getMemberId());
            }
            return this;
        }

        public Builder withLeaderRevision(ClusterMembershipRevision<ClusterMemberLeadership> leaderRevision) {
            this.leaderRevision = leaderRevision;
            return this;
        }

        public Builder withStalenessMs(long stalenessMs) {
            this.stalenessMs = stalenessMs;
            return this;
        }

        public ClusterMembershipSnapshot build() {
            return new ClusterMembershipSnapshot(memberRevisions, Optional.ofNullable(leaderRevision), stalenessMs);
        }
    }
}
