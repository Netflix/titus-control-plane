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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadershipState;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.api.health.HealthState;
import com.netflix.titus.api.health.HealthStatus;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.spectator.SpectatorExt;
import com.netflix.titus.common.util.time.Clock;

/**
 * Companion object to {@link DefaultClusterMembershipService}.
 */
final class ClusterMembershipServiceMetrics {

    public static final String METRIC_ROOT = "titus.clusterMembership.service.";

    private final Registry registry;
    private final Clock clock;

    private final SpectatorExt.FsmMetrics<ClusterMemberLeadershipState> leadershipFsmMetrics;
    private final SpectatorExt.FsmMetrics<HealthState> healthFsmMetrics;

    private final Id knownSiblingsId;
    private final AtomicLong knownSiblingsCounter = new AtomicLong();

    private final Id siblingStalenessId;
    private final ConcurrentMap<String, Gauge> siblingStaleness = new ConcurrentHashMap<>();

    ClusterMembershipServiceMetrics(TitusRuntime titusRuntime) {
        // known members count
        this.registry = titusRuntime.getRegistry();
        this.clock = titusRuntime.getClock();
        this.healthFsmMetrics = SpectatorExt.fsmMetrics(
                registry.createId(METRIC_ROOT + "localHealthState"),
                s -> false,
                HealthState.Unknown,
                registry
        );
        this.leadershipFsmMetrics = SpectatorExt.fsmMetrics(
                registry.createId(METRIC_ROOT + "localLeadershipState"),
                s -> false,
                ClusterMemberLeadershipState.Unknown,
                registry
        );

        this.knownSiblingsId = registry.createId(METRIC_ROOT + "knownSiblings");
        this.siblingStalenessId = registry.createId(METRIC_ROOT + "siblingStaleness");
        PolledMeter.using(registry).withId(knownSiblingsId).monitorValue(knownSiblingsCounter);
    }

    void shutdown() {
        PolledMeter.remove(registry, knownSiblingsId);
        updateSiblings(Collections.emptyMap());
    }

    void updateLocal(ClusterMemberLeadershipState localLeadershipState, HealthStatus health) {
        leadershipFsmMetrics.transition(localLeadershipState);
        healthFsmMetrics.transition(health.getHealthState());
    }

    void updateSiblings(Map<String, ClusterMembershipRevision<ClusterMember>> clusterMemberSiblings) {
        knownSiblingsCounter.set(clusterMemberSiblings.size());

        long now = clock.wallTime();
        clusterMemberSiblings.forEach((memberId, sibling) ->
                siblingStaleness.computeIfAbsent(
                        memberId,
                        id -> registry.gauge(siblingStalenessId.withTag("memberId", memberId))
                ).set(now - sibling.getTimestamp())
        );
        siblingStaleness.forEach((memberId, gauge) -> {
            if (!clusterMemberSiblings.containsKey(memberId)) {
                gauge.set(0);
            }
        });
        siblingStaleness.keySet().retainAll(clusterMemberSiblings.keySet());
    }
}
