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

package com.netflix.titus.ext.k8s.clustermembership.connector;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import javax.annotation.PreDestroy;
import javax.inject.Singleton;

import com.netflix.titus.api.clustermembership.connector.ClusterMembershipConnector;
import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadership;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipChangeEvent;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipEvent;
import com.netflix.titus.api.clustermembership.model.event.LeaderElectionChangeEvent;
import com.netflix.titus.common.framework.simplereconciler.SimpleReconciliationEngine;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.IOExt;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.ext.k8s.clustermembership.connector.action.K8LeaderElectionActions;
import com.netflix.titus.ext.k8s.clustermembership.connector.action.K8RegistrationActions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Singleton
public class K8ClusterMembershipConnector implements ClusterMembershipConnector {

    private static final Logger logger = LoggerFactory.getLogger(K8ClusterMembershipConnector.class);

    private final Scheduler scheduler;

    private final SimpleReconciliationEngine<K8ClusterState> reconciler;
    private final K8Context context;

    private final Disposable membershipSubscription;
    private final Disposable leaderElectionSubscription;
    private final Disposable reconcilerEventsSubscription;

    public K8ClusterMembershipConnector(ClusterMember initial,
                                        K8MembershipExecutor k8MembershipExecutor,
                                        K8LeaderElectionExecutor k8LeaderElectionExecutor,
                                        K8ConnectorConfiguration configuration,
                                        TitusRuntime titusRuntime) {
        this.scheduler = Schedulers.newSingle("ClusterMembershipReconciler");

        this.context = new K8Context(k8MembershipExecutor, k8LeaderElectionExecutor, titusRuntime);
        this.reconciler = SimpleReconciliationEngine.<K8ClusterState>newBuilder("LeaderElection")
                .withInitial(new K8ClusterState(initial, titusRuntime.getClock()))
                .withReconcilerActionsProvider(new K8ClusterMembershipStateReconciler(context, configuration))
                .withQuickCycle(Duration.ofMillis(configuration.getReconcilerQuickCycleMs()))
                .withLongCycle(Duration.ofMillis(configuration.getReconcilerLongCycleMs()))
                .withScheduler(scheduler)
                .withTitusRuntime(titusRuntime)
                .build();

        Duration reconnectInterval = Duration.ofMillis(configuration.getK8ReconnectIntervalMs());
        this.membershipSubscription = k8MembershipExecutor.watchMembershipEvents()
                .onErrorResume(e -> {
                            logger.info("Reconnecting membership event stream from K8S terminated with an error: {}", e.getMessage());
                            logger.debug("Stack trace", e);
                            return Flux.just(ClusterMembershipEvent.disconnectedEvent(e))
                                    .concatWith(Flux.interval(reconnectInterval).take(1).flatMap(tick -> k8MembershipExecutor.watchMembershipEvents()));
                        }
                )
                .subscribe(
                        event -> {
                            if (event instanceof ClusterMembershipChangeEvent) {
                                reconciler.apply(Mono.just(currentState -> currentState.processMembershipEventStreamEvent((ClusterMembershipChangeEvent) event)))
                                        .subscribe(
                                                next -> logger.info("Processed K8S event: {}", event),
                                                e -> logger.warn("K8S event processing failure", e)
                                        );
                            }
                        },
                        e -> logger.error("Unexpected error in the K8S membership event stream", e),
                        () -> logger.info("Membership K8S event stream closed")
                );
        this.leaderElectionSubscription = k8LeaderElectionExecutor.watchLeaderElectionProcessUpdates()
                .onErrorResume(e -> {
                            logger.info("Reconnecting leadership event stream from K8S terminated with an error: {}", e.getMessage());
                            logger.debug("Stack trace", e);
                            return Flux.just(ClusterMembershipEvent.disconnectedEvent(e))
                                    .concatWith(Flux.interval(reconnectInterval).take(1).flatMap(tick -> k8LeaderElectionExecutor.watchLeaderElectionProcessUpdates()));
                        }
                )
                .subscribe(
                        event -> {
                            if (event instanceof LeaderElectionChangeEvent) {
                                reconciler.apply(Mono.just(currentState -> currentState.processLeaderElectionEventStreamEvent((LeaderElectionChangeEvent) event)))
                                        .subscribe(
                                                next -> logger.info("Processed K8S event: {}", event),
                                                e -> logger.warn("K8S event processing failure", e)
                                        );
                            }
                        },
                        e -> logger.error("Unexpected error in the K8S membership event stream", e),
                        () -> logger.info("Membership K8S event stream closed")
                );

        this.reconcilerEventsSubscription = this.reconciler.changes().subscribe(
                next -> logger.info("Reconciler update: {}", next.getDeltaEvents()),
                e -> logger.warn("Reconciler event stream terminated with an error", e),
                () -> logger.warn("Reconciler event stream completed")
        );
    }

    @PreDestroy
    public void shutdown() {
        IOExt.closeSilently(reconciler);
        ReactorExt.safeDispose(scheduler, membershipSubscription, leaderElectionSubscription, reconcilerEventsSubscription);
    }

    @Override
    public ClusterMembershipRevision<ClusterMember> getLocalClusterMemberRevision() {
        return reconciler.getCurrent().getLocalMemberRevision();
    }

    @Override
    public Map<String, ClusterMembershipRevision<ClusterMember>> getClusterMemberSiblings() {
        return reconciler.getCurrent().getClusterMemberSiblings();
    }

    @Override
    public ClusterMembershipRevision<ClusterMemberLeadership> getLocalLeadershipRevision() {
        return reconciler.getCurrent().getLocalMemberLeadershipRevision();
    }

    @Override
    public Optional<ClusterMembershipRevision<ClusterMemberLeadership>> findCurrentLeader() {
        return reconciler.getCurrent().findCurrentLeader();
    }

    @Override
    public Mono<ClusterMembershipRevision<ClusterMember>> register(Function<ClusterMember, ClusterMembershipRevision<ClusterMember>> selfUpdate) {
        return reconciler.apply(Mono.defer(() ->
                K8RegistrationActions.register(context, reconciler.getCurrent(), selfUpdate))
        ).map(K8ClusterState::getLocalMemberRevision);
    }

    @Override
    public Mono<ClusterMembershipRevision<ClusterMember>> unregister(Function<ClusterMember, ClusterMembershipRevision<ClusterMember>> selfUpdate) {
        return reconciler.apply(Mono.defer(() ->
                K8RegistrationActions.unregister(context, reconciler.getCurrent(), selfUpdate))
        ).map(K8ClusterState::getLocalMemberRevision);
    }

    @Override
    public Mono<Void> joinLeadershipGroup() {
        return reconciler.apply(K8LeaderElectionActions.createJoinLeadershipGroupAction(context)).ignoreElement().cast(Void.class);
    }

    @Override
    public Mono<Boolean> leaveLeadershipGroup(boolean onlyNonLeader) {
        return reconciler.apply(K8LeaderElectionActions.createLeaveLeadershipGroupAction(context, onlyNonLeader))
                .map(currentState -> !currentState.isInLeaderElectionProcess());
    }

    @Override
    public Flux<ClusterMembershipEvent> membershipChangeEvents() {
        return Flux.defer(() -> {
            AtomicBoolean firstEmit = new AtomicBoolean(true);
            return reconciler.changes()
                    .flatMap(update -> firstEmit.getAndSet(false)
                            ? Flux.just(update.getSnapshotEvent())
                            : Flux.fromIterable(update.getDeltaEvents())
                    );
        });
    }
}
