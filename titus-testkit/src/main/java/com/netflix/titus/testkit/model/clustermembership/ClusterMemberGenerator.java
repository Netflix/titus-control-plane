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

import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMemberAddress;
import com.netflix.titus.api.clustermembership.model.ClusterMemberState;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.common.util.CollectionsExt;

public final class ClusterMemberGenerator {

    public static ClusterMember activeClusterMember(String memberId) {
        return ClusterMember.newBuilder()
                .withMemberId(memberId)
                .withState(ClusterMemberState.Active)
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

    public static ClusterMembershipRevision<ClusterMember> clusterMemberRegistrationRevision(ClusterMember current) {
        long now = System.currentTimeMillis();
        return ClusterMembershipRevision.<ClusterMember>newBuilder()
                .withCurrent(current.toBuilder()
                        .withLabels(CollectionsExt.copyAndAdd(current.getLabels(), "changedAt", "" + now))
                        .build()
                )
                .withCode("registered")
                .withMessage("Registering")
                .withTimestamp(now)
                .build();
    }

    public static ClusterMembershipRevision<ClusterMember> clusterMemberUnregistrationRevision(ClusterMember current) {
        long now = System.currentTimeMillis();
        return ClusterMembershipRevision.<ClusterMember>newBuilder()
                .withCurrent(current.toBuilder()
                        .withLabels(CollectionsExt.copyAndAdd(current.getLabels(), "changedAt", "" + now))
                        .build()
                )
                .withCode("unregistered")
                .withMessage("Unregistering")
                .withTimestamp(now)
                .build();
    }

    public static ClusterMembershipRevision<ClusterMember> clusterMemberUpdateRevision(ClusterMembershipRevision<ClusterMember> currentRevision) {
        long now = System.currentTimeMillis();
        ClusterMember current = currentRevision.getCurrent();

        return ClusterMembershipRevision.<ClusterMember>newBuilder()
                .withCurrent(current.toBuilder()
                        .withLabels(CollectionsExt.copyAndAdd(current.getLabels(), "changedAt", "" + now))
                        .build()
                )
                .withCode("updated")
                .withMessage("Random update to generate next revision number")
                .withRevision(currentRevision.getRevision() + 1)
                .withTimestamp(now)
                .build();
    }
}
