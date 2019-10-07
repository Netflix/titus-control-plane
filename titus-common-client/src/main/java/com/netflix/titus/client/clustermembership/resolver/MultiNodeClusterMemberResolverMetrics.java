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

package com.netflix.titus.client.clustermembership.resolver;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadership;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipSnapshot;
import com.netflix.titus.client.clustermembership.ClusterMembershipClientMetrics;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.time.Clock;

/**
 * Companion class to {@link MultiNodeClusterMemberResolver}.
 */
final class MultiNodeClusterMemberResolverMetrics {

    private static final String ROOT = ClusterMembershipClientMetrics.CLUSTER_MEMBERSHIP_CLIENT_METRICS_ROOT + "multiNodeResolver.";

    private final String serviceName;
    private final Clock clock;
    private final Registry registry;

    private final Map<String, DirectConnection> directConnections = new HashMap<>();
    private final Map<String, KnownMember> knownMembers = new HashMap<>();

    private Leader leader;

    MultiNodeClusterMemberResolverMetrics(String serviceName, TitusRuntime titusRuntime) {
        this.serviceName = serviceName;
        this.clock = titusRuntime.getClock();
        this.registry = titusRuntime.getRegistry();
    }

    void updateConnectedMembers(Map<String, DirectClusterMemberResolver> memberResolversByIpAddress) {
        memberResolversByIpAddress.forEach((memberIp, directResolver) -> {
            if (!directConnections.containsKey(memberIp)) {
                directConnections.put(memberIp, new DirectConnection(memberIp, directResolver));
            }
        });

        Set<String> toRemove = CollectionsExt.copyAndRemove(directConnections.keySet(), memberResolversByIpAddress.keySet());
        toRemove.forEach(memberIp -> directConnections.remove(memberIp).close());
    }

    void updateSnapshot(ClusterMembershipSnapshot snapshot) {
        // Members
        snapshot.getMemberRevisions().forEach((memberId, revision) -> {
            if (!knownMembers.containsKey(memberId)) {
                knownMembers.put(memberId, new KnownMember(memberId));
            }
        });

        Set<String> toRemove = CollectionsExt.copyAndRemove(knownMembers.keySet(), snapshot.getMemberRevisions().keySet());
        toRemove.forEach(memberIp -> knownMembers.remove(memberIp).close());

        // Leader
        ClusterMemberLeadership currentLeader = snapshot.getLeaderRevision().map(ClusterMembershipRevision::getCurrent).orElse(null);
        if (currentLeader != null) {
            if (leader == null) {
                this.leader = new Leader(currentLeader);
            } else {
                if (!leader.getMemberId().equals(currentLeader.getMemberId())) {
                    leader.close();
                    this.leader = new Leader(currentLeader);
                }
            }
        } else if (leader != null) {
            leader.close();
            leader = null;
        }
    }

    private class DirectConnection implements Closeable {

        private final DirectClusterMemberResolver directResolver;
        private final long startTime;

        private final Id connectionHealthId;
        private final Id connectionTimeId;

        private volatile boolean closed;

        private DirectConnection(String ipAddress, DirectClusterMemberResolver directResolver) {
            this.startTime = clock.wallTime();
            this.directResolver = directResolver;

            this.connectionHealthId = registry.createId(
                    ROOT + "directConnection.health",
                    "service", serviceName,
                    "memberIp", ipAddress
            );
            this.connectionTimeId = registry.createId(
                    ROOT + "directConnection.time",
                    "service", serviceName,
                    "memberIp", ipAddress
            );

            PolledMeter.using(registry).withId(connectionHealthId).monitorValue(this, self ->
                    closed ? 0 : (self.directResolver.isHealthy() ? 1 : 0)
            );
            PolledMeter.using(registry).withId(connectionTimeId).monitorValue(this, self ->
                    closed ? 0 : (clock.wallTime() - self.startTime)
            );
        }

        @Override
        public void close() {
            closed = true;
            PolledMeter.update(registry);

            PolledMeter.remove(registry, connectionHealthId);
            PolledMeter.remove(registry, connectionTimeId);
        }
    }

    private class KnownMember implements Closeable {

        private final long startTime;

        private final Id memberTimeId;

        private volatile boolean closed;

        private KnownMember(String memberId) {
            this.startTime = clock.wallTime();

            this.memberTimeId = registry.createId(
                    ROOT + "knownMember.time",
                    "service", serviceName,
                    "memberId", memberId
            );

            PolledMeter.using(registry).withId(memberTimeId).monitorValue(this, self ->
                    closed ? 0 : (clock.wallTime() - self.startTime)
            );
        }

        @Override
        public void close() {
            closed = true;
            PolledMeter.update(registry);

            PolledMeter.remove(registry, memberTimeId);
        }
    }

    private class Leader implements Closeable {

        private final long startTime;

        private final Id leaderTimeId;
        private final String memberId;

        private volatile boolean closed;

        private Leader(ClusterMemberLeadership leader) {
            this.memberId = leader.getMemberId();
            this.startTime = clock.wallTime();

            this.leaderTimeId = registry.createId(
                    ROOT + "leader.time",
                    "service", serviceName,
                    "memberId", memberId
            );

            PolledMeter.using(registry).withId(leaderTimeId).monitorValue(this, self ->
                    closed ? 0 : (clock.wallTime() - self.startTime)
            );
        }

        private String getMemberId() {
            return memberId;
        }

        @Override
        public void close() {
            closed = true;
            PolledMeter.update(registry);

            PolledMeter.remove(registry, leaderTimeId);
        }
    }
}
