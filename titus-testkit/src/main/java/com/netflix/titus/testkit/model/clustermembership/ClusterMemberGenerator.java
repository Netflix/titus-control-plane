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

package com.netflix.titus.testkit.model.clustermembership;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMemberAddress;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadershipState;
import com.netflix.titus.api.clustermembership.model.ClusterMemberState;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;

public final class ClusterMemberGenerator {

    public static Map<String, ClusterMembershipRevision<ClusterMember>> newCluster(String memberIdPrefix, int size) {
        ClusterMember reference = activeClusterMember();
        Map<String, ClusterMembershipRevision<ClusterMember>> result = new HashMap<>();
        for (int i = 0; i < size; i++) {
            ClusterMember member = reference.toBuilder()
                    .withMemberId(memberIdPrefix + i)
                    .withLeadershipState(i == 0 ? ClusterMemberLeadershipState.Leader : ClusterMemberLeadershipState.NonLeader)
                    .build();
            ClusterMembershipRevision<ClusterMember> revision = ClusterMembershipRevision.<ClusterMember>newBuilder()
                    .withCurrent(member)
                    .withCode("initial")
                    .withMessage("Test")
                    .withTimestamp(System.currentTimeMillis())
                    .build();
            result.put(member.getMemberId(), revision);
        }
        return result;
    }

    public static ClusterMember activeClusterMember() {
        return activeClusterMember("member1");
    }

    public static ClusterMember activeClusterMember(String memberId) {
        return ClusterMember.newBuilder()
                .withMemberId(memberId)
                .withState(ClusterMemberState.Active)
                .withLeadershipState(ClusterMemberLeadershipState.NonLeader)
                .withEnabled(true)
                .withClusterMemberAddresses(Collections.singletonList(
                        ClusterMemberAddress.newBuilder()
                                .withIpAddress("10.0.0.1")
                                .withPortNumber(8081)
                                .withProtocol("https")
                                .withSecure(true)
                                .withDescription("REST endpoint")
                                .build()
                ))
                .withLabels(Collections.singletonMap("resourceVersion", "1"))
                .build();
    }

    public static ClusterMembershipRevision<ClusterMember> clusterMemberRevision(ClusterMember clusterMember) {
        return ClusterMembershipRevision.<ClusterMember>newBuilder()
                .withCurrent(clusterMember)
                .withCode("initial")
                .withMessage("Test")
                .withTimestamp(System.currentTimeMillis())
                .build();
    }
}
