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

package com.netflix.titus.runtime.clustermembership.endpoint.grpc;

import java.util.stream.Collectors;

import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMemberAddress;
import com.netflix.titus.grpc.protogen.ClusterMember.LeadershipState;
import com.netflix.titus.grpc.protogen.ClusterMembershipRevision;

public class ClusterMembershipGrpcConverters {

    public static ClusterMembershipRevision toGrpcClusterMembershipRevision(com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision<ClusterMember> memberRevision,
                                                                            boolean leader) {
        return ClusterMembershipRevision.newBuilder()
                .setCurrent(toGrpcClusterMember(memberRevision.getCurrent(), leader))
                .setCode(memberRevision.getCode())
                .setMessage(memberRevision.getMessage())
                .setTimestamp(memberRevision.getTimestamp())
                .build();
    }

    public static com.netflix.titus.grpc.protogen.ClusterMember toGrpcClusterMember(ClusterMember clusterMember, boolean leader) {
        LeadershipState leadershipState;
        if (leader) {
            leadershipState = LeadershipState.Leader;
        } else if (clusterMember.isActive()) {
            leadershipState = LeadershipState.NonLeader;
        } else {
            leadershipState = LeadershipState.Disabled;
        }

        return com.netflix.titus.grpc.protogen.ClusterMember.newBuilder()
                .setMemberId(clusterMember.getMemberId())
                .setEnabled(clusterMember.isEnabled())
                .setRegistered(clusterMember.isRegistered())
                .setLeadershipState(leadershipState)
                .addAllAddresses(clusterMember.getClusterMemberAddresses().stream()
                        .map(ClusterMembershipGrpcConverters::toGrpcServiceAddress)
                        .collect(Collectors.toList())
                )
                .putAllLabels(clusterMember.getLabels())
                .build();
    }

    public static com.netflix.titus.grpc.protogen.ClusterMemberAddress toGrpcServiceAddress(ClusterMemberAddress address) {
        return com.netflix.titus.grpc.protogen.ClusterMemberAddress.newBuilder()
                .setIpAddress(address.getIpAddress())
                .setPortNumber(address.getPortNumber())
                .setProtocol(address.getProtocol())
                .setSecure(address.isSecure())
                .setDescription(address.getDescription())
                .build();
    }
}
