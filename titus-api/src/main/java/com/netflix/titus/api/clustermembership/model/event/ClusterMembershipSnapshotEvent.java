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

public class ClusterMembershipSnapshotEvent extends ClusterMembershipEvent {

    private final List<ClusterMembershipRevision<ClusterMember>> clusterMemberRevisions;
    private final ClusterMembershipRevision<ClusterMemberLeadership> localLeadership;

    ClusterMembershipSnapshotEvent(List<ClusterMembershipRevision<ClusterMember>> clusterMemberRevisions,
                                   ClusterMembershipRevision<ClusterMemberLeadership> localLeadership) {
        this.clusterMemberRevisions = clusterMemberRevisions;
        this.localLeadership = localLeadership;
    }

    public List<ClusterMembershipRevision<ClusterMember>> getClusterMemberRevisions() {
        return clusterMemberRevisions;
    }

    public ClusterMembershipRevision<ClusterMemberLeadership> getLocalLeadership() {
        return localLeadership;
    }
}
