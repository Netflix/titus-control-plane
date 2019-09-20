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
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadershipState;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipEvent;
import com.netflix.titus.api.clustermembership.service.ClusterMembershipService;
import com.netflix.titus.api.clustermembership.service.ClusterMembershipServiceException;
import com.netflix.titus.api.health.HealthIndicator;
import com.netflix.titus.api.health.HealthState;
import com.netflix.titus.api.health.HealthStatus;
import com.netflix.titus.common.framework.scheduler.ExecutionContext;
import com.netflix.titus.common.framework.scheduler.ScheduleReference;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.retry.Retryers;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.time.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Singleton
public class DefaultClusterMembershipService implements ClusterMembershipService {

    private static final Logger logger = LoggerFactory.getLogger(DefaultClusterMembershipService.class);

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

    private final ClusterMembershipServiceConfiguration configuration;
    private final ClusterMembershipConnector connector;
    private final HealthIndicator healthIndicator;
    private final ClusterMembershipServiceMetrics metrics;
    private final Clock clock;

    private final ScheduleReference healthScheduleRef;

    private final Disposable transactionLogDisposable;

    @Inject
    public DefaultClusterMembershipService(ClusterMembershipServiceConfiguration configuration,
                                           ClusterMembershipConnector connector,
                                           HealthIndicator healthIndicator,
                                           TitusRuntime titusRuntime) {
        this.configuration = configuration;
        this.connector = connector;
        this.healthIndicator = healthIndicator;
        this.metrics = new ClusterMembershipServiceMetrics(titusRuntime);
        this.clock = titusRuntime.getClock();

        this.healthScheduleRef = titusRuntime.getLocalScheduler().scheduleMono(
                HEALTH_CHECK_SCHEDULE_DESCRIPTOR.toBuilder()
                        .withInterval(Duration.ofMillis(configuration.getHealthCheckEvaluationIntervalMs()))
                        .withTimeout(Duration.ofMillis(configuration.getHealthCheckEvaluationTimeoutMs()))
                        .build(),
                this::clusterStateEvaluator,
                Schedulers.parallel()
        );

        this.transactionLogDisposable = ClusterMembershipTransactionLogger.logEvents(connector.membershipChangeEvents());
    }

    public void shutdown() {
        metrics.shutdown();
        healthScheduleRef.cancel();
        ReactorExt.safeDispose(transactionLogDisposable);
    }

    @Override
    public ClusterMembershipRevision<ClusterMember> getLocalClusterMember() {
        try {
            return connector.getLocalClusterMemberRevision();
        } catch (Exception e) {
            throw ClusterMembershipServiceException.internalError(e);
        }
    }

    @Override
    public Map<String, ClusterMembershipRevision<ClusterMember>> getClusterMemberSiblings() {
        try {
            return connector.getClusterMemberSiblings();
        } catch (Exception e) {
            throw ClusterMembershipServiceException.internalError(e);
        }
    }

    @Override
    public ClusterMembershipRevision<ClusterMemberLeadership> getLocalLeadership() {
        try {
            return connector.getLocalLeadershipRevision();
        } catch (Exception e) {
            throw ClusterMembershipServiceException.internalError(e);
        }
    }

    @Override
    public Optional<ClusterMembershipRevision<ClusterMemberLeadership>> findLeader() {
        try {
            return connector.findCurrentLeader();
        } catch (Exception e) {
            throw ClusterMembershipServiceException.internalError(e);
        }
    }

    @Override
    public Mono<ClusterMembershipRevision<ClusterMember>> updateSelf(Function<ClusterMember, ClusterMembershipRevision<ClusterMember>> memberUpdate) {
        return connector.register(memberUpdate).onErrorMap(ClusterMembershipServiceException::selfUpdateError);
    }

    @Override
    public Mono<Void> stopBeingLeader() {
        return connector.leaveLeadershipGroup(false)
                .ignoreElement()
                .cast(Void.class)
                .onErrorMap(ClusterMembershipServiceException::internalError);
    }

    @Override
    public Flux<ClusterMembershipEvent> events() {
        return connector.membershipChangeEvents();
    }

    private Mono<Void> clusterStateEvaluator(ExecutionContext context) {
        return Mono.defer(() -> {
            ClusterMember localMember = connector.getLocalClusterMemberRevision().getCurrent();
            ClusterMemberLeadershipState localLeadershipState = connector.getLocalLeadershipRevision().getCurrent().getLeadershipState();
            HealthStatus health = healthIndicator.health();

            // Explicitly disabled
            if (!configuration.isLeaderElectionEnabled() || !localMember.isEnabled()) {
                if (localLeadershipState == ClusterMemberLeadershipState.NonLeader) {
                    logger.info("Local member excluded from the leader election. Leaving the leader election process");
                    return connector.leaveLeadershipGroup(true).flatMap(success ->
                            success
                                    ? connector.register(current -> toInactive(current, "Marked by a user as disabled")).ignoreElement().cast(Void.class)
                                    : Mono.empty()
                    );
                }
                if (localLeadershipState == ClusterMemberLeadershipState.Disabled && localMember.isActive()) {
                    return connector.register(current -> toInactive(current, "Marked by a user as disabled")).ignoreElement().cast(Void.class);
                }
                return Mono.empty();
            }

            // Re-enable if healthy
            if (health.getHealthState() == HealthState.Healthy) {
                if (localLeadershipState == ClusterMemberLeadershipState.Disabled) {
                    logger.info("Re-enabling local member which is in the disabled state");
                    return connector.joinLeadershipGroup()
                            .then(connector.register(this::toActive).ignoreElement().cast(Void.class));
                }
                if (!localMember.isActive()) {
                    return connector.register(this::toActive).ignoreElement().cast(Void.class);
                }
                return Mono.empty();
            }

            // Disable if unhealthy (and not the leader)
            if (localLeadershipState != ClusterMemberLeadershipState.Disabled && localLeadershipState != ClusterMemberLeadershipState.Leader) {
                logger.info("Disabling local member as it is unhealthy: {}", health);
                return connector.leaveLeadershipGroup(true).flatMap(success ->
                        success
                                ? connector.register(current -> toInactive(current, "Unhealthy: " + health)).ignoreElement().cast(Void.class)
                                : Mono.empty()
                );
            }
            if (localLeadershipState == ClusterMemberLeadershipState.Disabled && localMember.isActive()) {
                return connector.register(current -> toInactive(current, "Unhealthy: " + health)).ignoreElement().cast(Void.class);
            }
            return Mono.empty();
        }).doOnError(error -> {
            logger.info("Cluster membership health evaluation error: {}", error.getMessage());
            logger.debug("Stack trace", error);
        }).doOnTerminate(() -> {
            metrics.updateLocal(
                    connector.getLocalLeadershipRevision().getCurrent().getLeadershipState(),
                    healthIndicator.health()
            );
            metrics.updateSiblings(connector.getClusterMemberSiblings());
        });
    }

    private ClusterMembershipRevision<ClusterMember> toActive(ClusterMember current) {
        return ClusterMembershipRevision.<ClusterMember>newBuilder()
                .withCurrent(current.toBuilder().withActive(true).build())
                .withCode("activated")
                .withMessage("Activated")
                .withTimestamp(clock.wallTime())
                .build();
    }

    private ClusterMembershipRevision<ClusterMember> toInactive(ClusterMember current, String cause) {
        return ClusterMembershipRevision.<ClusterMember>newBuilder()
                .withCurrent(current.toBuilder().withActive(false).build())
                .withCode("deactivated")
                .withMessage(cause)
                .withTimestamp(clock.wallTime())
                .build();
    }
}
