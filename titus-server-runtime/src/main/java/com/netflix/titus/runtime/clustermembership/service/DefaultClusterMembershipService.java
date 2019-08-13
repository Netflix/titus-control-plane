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

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.clustermembership.connector.ClusterMembershipConnector;
import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadership;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipEvent;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipSnapshotEvent;
import com.netflix.titus.api.clustermembership.service.ClusterMembershipService;
import com.netflix.titus.api.health.HealthIndicator;
import com.netflix.titus.api.health.HealthState;
import com.netflix.titus.api.health.HealthStatus;
import com.netflix.titus.common.framework.scheduler.ExecutionContext;
import com.netflix.titus.common.framework.scheduler.ScheduleReference;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.retry.Retryers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Singleton
public class DefaultClusterMembershipService implements ClusterMembershipService {

    private static final Duration HEALTH_CHECK_INITIAL = Duration.ofMillis(1000);
    private static final Duration HEALTH_CHECK_TIMEOUT = Duration.ofMillis(2000);

    private static final ScheduleDescriptor HEALTH_CHECK_SCHEDULE_DESCRIPTOR = ScheduleDescriptor.newBuilder()
            .withName("ClusterMembershipHealthCheck")
            .withInitialDelay(Duration.ZERO)
            .withInterval(HEALTH_CHECK_INITIAL)
            .withTimeout(HEALTH_CHECK_TIMEOUT)
            .withDescription("Evaluates local member health and updates the membership state if needed")
            .withRetryerSupplier(Retryers::never)
            .build();

    private final ClusterMembershipConnector connector;
    private final HealthIndicator healthIndicator;

    private final ScheduleReference healthScheduleRef;

    @Inject
    public DefaultClusterMembershipService(ClusterMembershipConnector connector,
                                           HealthIndicator healthIndicator,
                                           TitusRuntime titusRuntime) {
        this.connector = connector;
        this.healthIndicator = healthIndicator;

        this.healthScheduleRef = titusRuntime.getLocalScheduler().scheduleMono(
                HEALTH_CHECK_SCHEDULE_DESCRIPTOR,
                this::evaluateHealth,
                Schedulers.parallel()
        );
    }

    public void shutdown() {
        healthScheduleRef.cancel();
    }

    @Override
    public ClusterMembershipRevision<ClusterMember> getLocalClusterMember() {
        return connector.getLocalClusterMemberRevision();
    }

    @Override
    public Map<String, ClusterMembershipRevision<ClusterMember>> getClusterMemberSiblings() {
        return connector.getClusterMemberSiblings();
    }

    @Override
    public ClusterMembershipRevision<ClusterMemberLeadership> getLocalLeadership() {
        return connector.getLocalLeadershipRevision();
    }

    @Override
    public Optional<ClusterMembershipRevision<ClusterMemberLeadership>> findLeader() {
        return connector.findCurrentLeader();
    }

    @Override
    public Mono<ClusterMembershipRevision<ClusterMember>> updateSelf(Function<ClusterMember, ClusterMembershipRevision<ClusterMember>> memberUpdate) {
        return connector.register(memberUpdate);
    }

    @Override
    public Mono<Void> stopBeingLeader() {
        return connector.leaveLeadershipGroup(false).ignoreElement().cast(Void.class);
    }

    @Override
    public Flux<ClusterMembershipEvent> events(boolean snapshot) {
        return connector.membershipChangeEvents().filter(event -> !(event instanceof ClusterMembershipSnapshotEvent));
    }

    private Mono<Void> evaluateHealth(ExecutionContext context) {
        return Mono.defer(() -> {
            HealthStatus health = healthIndicator.health();
            return health.getHealthState() == HealthState.Healthy
                    ? connector.joinLeadershipGroup()
                    : connector.leaveLeadershipGroup(true).ignoreElement().cast(Void.class);
        });
    }
}
