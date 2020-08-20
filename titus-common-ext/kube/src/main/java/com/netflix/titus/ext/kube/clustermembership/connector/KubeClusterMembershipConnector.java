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

package com.netflix.titus.ext.kube.clustermembership.connector;

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
import com.netflix.titus.common.framework.simplereconciler.OneOffReconciler;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.ext.kube.clustermembership.connector.action.KubeLeaderElectionActions;
import com.netflix.titus.ext.kube.clustermembership.connector.action.KubeRegistrationActions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Singleton
public class KubeClusterMembershipConnector implements ClusterMembershipConnector {

    private static final Logger logger = LoggerFactory.getLogger(KubeClusterMembershipConnector.class);

    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofMillis(500000);

    private final Scheduler scheduler;

    private final OneOffReconciler<KubeClusterState> reconciler;
    private final KubeContext context;

    private final Disposable membershipSubscription;
    private final Disposable leaderElectionSubscription;
    private final Disposable reconcilerEventsSubscription;

    public KubeClusterMembershipConnector(ClusterMember initial,
                                          KubeMembershipExecutor kubeMembershipExecutor,
                                          KubeLeaderElectionExecutor kubeLeaderElectionExecutor,
                                          KubeClusterMembershipConfiguration configuration,
                                          TitusRuntime titusRuntime) {
        this.scheduler = Schedulers.newSingle("ClusterMembershipReconciler");

        this.context = new KubeContext(kubeMembershipExecutor, kubeLeaderElectionExecutor, titusRuntime);
        this.reconciler = OneOffReconciler.<KubeClusterState>newBuilder("LeaderElection")
                .withInitial(new KubeClusterState(initial, configuration, titusRuntime.getClock()))
                .withReconcilerActionsProvider(new KubeClusterMembershipStateReconciler(context, configuration))
                .withQuickCycle(Duration.ofMillis(configuration.getReconcilerQuickCycleMs()))
                .withLongCycle(Duration.ofMillis(configuration.getReconcilerLongCycleMs()))
                .withScheduler(scheduler)
                .withTitusRuntime(titusRuntime)
                .build();

        Duration reconnectInterval = Duration.ofMillis(configuration.getKubeReconnectIntervalMs());
        this.membershipSubscription = kubeMembershipExecutor.watchMembershipEvents()
                .materialize().flatMap(signal -> {
                    if(signal.getType() == SignalType.ON_NEXT) {
                        return Mono.just(signal.get());
                    }
                    if(signal.getType() == SignalType.ON_ERROR) {
                        return Mono.error(signal.getThrowable());
                    }
                    if(signal.getType() == SignalType.ON_COMPLETE) {
                        return Mono.error(new IllegalStateException("watchMembershipEvents completed, and must be restarted"));
                    }
                    return Mono.empty();
                })
                .retryWhen(errors -> errors.flatMap(e -> {
                    logger.info("Reconnecting membership event stream from Kubernetes terminated with an error: {}", e.getMessage());
                    logger.debug("Stack trace", e);
                    return Flux.interval(reconnectInterval).take(1);
                }))
                .subscribe(
                        event -> {
                            if (event instanceof ClusterMembershipChangeEvent) {
                                reconciler.apply(currentState -> Mono.just(currentState.processMembershipEventStreamEvent((ClusterMembershipChangeEvent) event)))
                                        .subscribe(
                                                next -> logger.info("Processed Kubernetes event: {}", event),
                                                e -> logger.warn("Kubernetes event processing failure", e)
                                        );
                            }
                        },
                        e -> logger.error("Unexpected error in the Kubernetes membership event stream", e),
                        () -> logger.info("Membership Kubernetes event stream closed")
                );
        this.leaderElectionSubscription = kubeLeaderElectionExecutor.watchLeaderElectionProcessUpdates()
                .materialize().flatMap(signal -> {
                    if(signal.getType() == SignalType.ON_NEXT) {
                        return Mono.just(signal.get());
                    }
                    if(signal.getType() == SignalType.ON_ERROR) {
                        return Mono.error(signal.getThrowable());
                    }
                    if(signal.getType() == SignalType.ON_COMPLETE) {
                        return Mono.error(new IllegalStateException("watchLeaderElectionProcessUpdates completed, and must be restarted"));
                    }
                    return Mono.empty();
                })
                .retryWhen(errors -> errors.flatMap(e -> {
                    logger.info("Reconnecting leadership event stream from Kubernetes terminated with an error: {}", e.getMessage());
                    logger.debug("Stack trace", e);
                    return Flux.interval(reconnectInterval).take(1);
                }))
                .subscribe(
                        event -> {
                            if (event instanceof LeaderElectionChangeEvent) {
                                reconciler.apply(currentState -> Mono.just(currentState.processLeaderElectionEventStreamEvent((LeaderElectionChangeEvent) event)))
                                        .subscribe(
                                                next -> logger.debug("Processed Kubernetes event: {}", event),
                                                e -> logger.warn("Kubernetes event processing failure", e)
                                        );
                            }
                        },
                        e -> logger.error("Unexpected error in the Kubernetes membership event stream", e),
                        () -> logger.info("Membership Kubernetes event stream closed")
                );

        this.reconcilerEventsSubscription = this.reconciler.changes().subscribe(
                next -> logger.debug("Reconciler update: {}", next.getDeltaEvents()),
                e -> logger.warn("Reconciler event stream terminated with an error", e),
                () -> logger.warn("Reconciler event stream completed")
        );
    }

    @PreDestroy
    public void shutdown() {
        reconciler.close().block(SHUTDOWN_TIMEOUT);
        ReactorExt.safeDispose(scheduler, membershipSubscription, leaderElectionSubscription, reconcilerEventsSubscription);
    }

    @Override
    public ClusterMembershipRevision<ClusterMember> getLocalClusterMemberRevision() {
        return reconciler.getCurrent().getLocalMemberRevision();
    }

    @Override
    public Map<String, ClusterMembershipRevision<ClusterMember>> getClusterMemberSiblings() {
        return reconciler.getCurrent().getNotStaleClusterMemberSiblings();
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
        return reconciler.apply(current ->
                KubeRegistrationActions.registerLocal(context, current, selfUpdate).map(f -> f.apply(current)))
                .map(KubeClusterState::getLocalMemberRevision);
    }

    @Override
    public Mono<ClusterMembershipRevision<ClusterMember>> unregister(Function<ClusterMember, ClusterMembershipRevision<ClusterMember>> selfUpdate) {
        return reconciler.apply(current ->
                KubeRegistrationActions.unregisterLocal(context, current, selfUpdate).map(f -> f.apply(current))
        ).map(KubeClusterState::getLocalMemberRevision);
    }

    @Override
    public Mono<Void> joinLeadershipGroup() {
        return reconciler.apply(clusterState ->
                KubeLeaderElectionActions.createJoinLeadershipGroupAction(context).map(f -> f.apply(clusterState))
        ).ignoreElement().cast(Void.class);
    }

    @Override
    public Mono<Boolean> leaveLeadershipGroup(boolean onlyNonLeader) {
        return reconciler.apply(clusterState ->
                KubeLeaderElectionActions.createLeaveLeadershipGroupAction(context, onlyNonLeader).map(f -> f.apply(clusterState))
        ).map(currentState -> !currentState.isInLeaderElectionProcess());
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
