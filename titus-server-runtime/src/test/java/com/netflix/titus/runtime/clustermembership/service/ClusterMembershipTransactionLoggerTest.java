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

package com.netflix.titus.runtime.clustermembership.service;

import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadership;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadershipState;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipEvent;
import org.junit.Test;

import static com.netflix.titus.testkit.model.clustermembership.ClusterMemberGenerator.activeClusterMember;
import static com.netflix.titus.testkit.model.clustermembership.ClusterMemberGenerator.clusterMemberRegistrationRevision;
import static org.assertj.core.api.Assertions.assertThat;

public class ClusterMembershipTransactionLoggerTest {

    @Test
    public void testMemberChangeEventLogging() {
        ClusterMembershipRevision<ClusterMember> revision = clusterMemberRegistrationRevision(activeClusterMember("local"));
        String value = ClusterMembershipTransactionLogger.doFormat(ClusterMembershipEvent.memberAddedEvent(revision));
        assertThat(value).isEqualTo("memberId=local memberState=Active   registered=false enabled=true memberRevision=0        leadershipState=n/a        leadershipRevision=n/a");
    }

    @Test
    public void testLeadershipChangeEventLogging() {
        ClusterMembershipRevision<ClusterMemberLeadership> revision = ClusterMembershipRevision.<ClusterMemberLeadership>newBuilder()
                .withCurrent(ClusterMemberLeadership.newBuilder()
                        .withMemberId("local")
                        .withLeadershipState(ClusterMemberLeadershipState.Leader)
                        .build()
                )
                .build();
        String value = ClusterMembershipTransactionLogger.doFormat(ClusterMembershipEvent.leaderElected(revision));
        assertThat(value).isEqualTo("memberId=local memberState=n/a      registered=n/a   enabled=n/a  memberRevision=n/a      leadershipState=Leader     leadershipRevision=0");
    }
}