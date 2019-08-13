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

import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;

public class ClusterMembershipChangeEvent extends ClusterMembershipEvent {

    public enum ChangeType {
        Added,
        Updated,
        Removed
    }

    private final ChangeType changeType;
    private final ClusterMembershipRevision<ClusterMember> revision;

    public ClusterMembershipChangeEvent(ChangeType changeType, ClusterMembershipRevision<ClusterMember> revision) {
        this.changeType = changeType;
        this.revision = revision;
    }

    public ChangeType getChangeType() {
        return changeType;
    }

    public ClusterMembershipRevision<ClusterMember> getRevision() {
        return revision;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClusterMembershipChangeEvent that = (ClusterMembershipChangeEvent) o;
        return changeType == that.changeType &&
                Objects.equals(revision, that.revision);
    }

    @Override
    public int hashCode() {
        return Objects.hash(changeType, revision);
    }

    @Override
    public String toString() {
        return "ClusterMembershipChangeEvent{" +
                "changeType=" + changeType +
                ", revision=" + revision +
                '}';
    }
}
