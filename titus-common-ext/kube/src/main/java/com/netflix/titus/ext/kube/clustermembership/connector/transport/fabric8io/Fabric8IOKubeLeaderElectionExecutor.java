/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.ext.kube.clustermembership.connector.transport.fabric8io;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.PreDestroy;

import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadership;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadershipState;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipEvent;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.DateTimeExt;
import com.netflix.titus.common.util.IOExt;
import com.netflix.titus.common.util.rx.RetryHandlerBuilder;
import com.netflix.titus.common.util.spectator.ActionMetrics;
import com.netflix.titus.common.util.spectator.SpectatorExt;
import com.netflix.titus.ext.kube.clustermembership.connector.KubeLeaderElectionExecutor;
import com.netflix.titus.ext.kube.clustermembership.connector.KubeMetrics;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderCallbacks;
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElectionConfigBuilder;
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElector;
import io.fabric8.kubernetes.client.extended.leaderelection.resourcelock.LeaderElectionRecord;
import io.fabric8.kubernetes.client.extended.leaderelection.resourcelock.LeaseLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.ReplayProcessor;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * The leader election process and the current leader discovery are handled separately. The former is handled by
 * Kubernetes {@link LeaderElector} object, which only tells when the local node becomes a leader. To get
 * information about who is the leader, we check periodically the associated {@link LeaseLock}.
 */
public class Fabric8IOKubeLeaderElectionExecutor implements KubeLeaderElectionExecutor {

    private static final Logger logger = LoggerFactory.getLogger(Fabric8IOKubeLeaderElectionExecutor.class);

    private static final Duration LEADER_POLL_INTERVAL = Duration.ofSeconds(1);
    private static final int LEADER_POLL_RETRIES = 3;

    private static final AtomicInteger LEADER_ELECTION_THREAD_IDX = new AtomicInteger();
    private static final AtomicInteger WATCHER_THREAD_IDX = new AtomicInteger();

    private final NamespacedKubernetesClient kubeApiClient;
    private final String namespace;
    private final String clusterName;
    private final Duration leaseDuration;
    private final String localMemberId;
    private final Duration renewDeadline;
    private final Duration retryPeriod;
    private final TitusRuntime titusRuntime;

    private final AtomicReference<LeaderElectionHandler> leaderElectionHandlerRef = new AtomicReference<>();
    private final ReplayProcessor<LeaderElectionHandler> handlerProcessor = ReplayProcessor.create();

    // Metrics
    private final Registry registry;
    private final Id inLeaderElectionProcessMetricId;
    private final Id isLeaderMetricId;

    private final ActionMetrics lastElectionAttemptAction;
    private final ActionMetrics electedLeaderRefreshAction;

    private final LeaseLock readOnlyEndpointsLock;

    public Fabric8IOKubeLeaderElectionExecutor(NamespacedKubernetesClient kubeApiClient,
                                               String namespace,
                                               String clusterName,
                                               Duration leaseDuration,
                                               String localMemberId,
                                               TitusRuntime titusRuntime) {
        this.kubeApiClient = kubeApiClient;
        this.namespace = namespace;
        this.clusterName = clusterName;
        this.leaseDuration = leaseDuration;
        this.renewDeadline = leaseDuration.dividedBy(2);
        this.retryPeriod = leaseDuration.dividedBy(5);
        this.localMemberId = localMemberId;

        this.registry = titusRuntime.getRegistry();
        List<Tag> tags = Arrays.asList(
                new BasicTag("kubeNamespace", namespace),
                new BasicTag("kubeCluster", clusterName),
                new BasicTag("memberId", localMemberId)
        );

        this.inLeaderElectionProcessMetricId = registry.createId(KubeMetrics.KUBE_METRIC_ROOT + "inLeaderElectionProcess", tags);
        this.isLeaderMetricId = registry.createId(KubeMetrics.KUBE_METRIC_ROOT + "isLeader", tags);
        PolledMeter.using(registry).withId(inLeaderElectionProcessMetricId).monitorValue(this, self -> self.isInLeaderElectionProcess() ? 1 : 0);
        PolledMeter.using(registry).withId(isLeaderMetricId).monitorValue(this, self -> self.isLeader() ? 1 : 0);

        Id lastElectionAttemptMetricId = registry.createId(KubeMetrics.KUBE_METRIC_ROOT + "lastElectionAttempt", tags);
        Id electedLeaderRefreshId = registry.createId(KubeMetrics.KUBE_METRIC_ROOT + "electedLeaderRefresh", tags);
        this.lastElectionAttemptAction = SpectatorExt.actionMetrics(lastElectionAttemptMetricId, registry);
        this.electedLeaderRefreshAction = SpectatorExt.actionMetrics(electedLeaderRefreshId, registry);

        this.titusRuntime = titusRuntime;
        this.readOnlyEndpointsLock = new LeaseLock(namespace, clusterName, localMemberId);
    }

    @PreDestroy
    public void shutdown() {
        PolledMeter.remove(registry, inLeaderElectionProcessMetricId);
        PolledMeter.remove(registry, isLeaderMetricId);
        IOExt.closeSilently(lastElectionAttemptAction, electedLeaderRefreshAction);
    }

    @Override
    public boolean isInLeaderElectionProcess() {
        LeaderElectionHandler handler = leaderElectionHandlerRef.get();
        return handler != null && !handler.isDone();
    }

    @Override
    public boolean isLeader() {
        LeaderElectionHandler handler = leaderElectionHandlerRef.get();
        return handler != null && handler.isLeader();
    }

    @Override
    public boolean joinLeaderElectionProcess() {
        synchronized (leaderElectionHandlerRef) {
            if (leaderElectionHandlerRef.get() != null && !leaderElectionHandlerRef.get().isDone()) {
                return false;
            }

            LeaderElectionHandler newHandler = new LeaderElectionHandler();
            leaderElectionHandlerRef.set(newHandler);
            handlerProcessor.onNext(newHandler);
        }
        return true;
    }

    @Override
    public void leaveLeaderElectionProcess() {
        LeaderElectionHandler current = leaderElectionHandlerRef.get();
        if (current != null) {
            current.leave();
        }
    }

    /**
     * Kubernetes leader lock observer.
     */
    @Override
    public Flux<ClusterMembershipEvent> watchLeaderElectionProcessUpdates() {
        Flux<ClusterMembershipEvent> lockWatcher = Flux.defer(() -> {
            Scheduler singleScheduler = Schedulers.newSingle("LeaderWatcher-" + WATCHER_THREAD_IDX.getAndIncrement());
            return Flux.interval(LEADER_POLL_INTERVAL, singleScheduler)
                    .flatMap(tick -> {
                        long started = electedLeaderRefreshAction.start();
                        ClusterMembershipRevision<ClusterMemberLeadership> revision;
                        try {
                            revision = refreshCurrentLeaderRevision();
                            electedLeaderRefreshAction.finish(started);
                        } catch (Exception e) {
                            Throwable unwrapped = e.getCause() != null ? e.getCause() : e;
                            electedLeaderRefreshAction.failure(unwrapped);
                            return Flux.error(unwrapped);
                        }
                        return Flux.<ClusterMembershipEvent>just(ClusterMembershipEvent.leaderElected(revision));
                    })
                    .retryWhen(RetryHandlerBuilder.retryHandler()
                            .withRetryCount(LEADER_POLL_RETRIES)
                            .withDelay(Math.max(1, leaseDuration.toMillis() / 10), leaseDuration.toMillis(), TimeUnit.MILLISECONDS)
                            .withReactorScheduler(Schedulers.parallel())
                            .buildRetryExponentialBackoff()
                    )
                    .doOnCancel(singleScheduler::dispose)
                    .doAfterTerminate(singleScheduler::dispose);
        }).distinctUntilChanged();

        return lockWatcher.mergeWith(handlerProcessor.flatMap(LeaderElectionHandler::events));
    }

    private ClusterMembershipRevision<ClusterMemberLeadership> refreshCurrentLeaderRevision() {
        try {
            return createClusterMemberLeadershipObject(readOnlyEndpointsLock.get(kubeApiClient), readOnlyEndpointsLock.get(kubeApiClient).getHolderIdentity());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Build {@link ClusterMemberLeadership} after the local node became a leader.
     */
    private ClusterMembershipRevision<ClusterMemberLeadership> createLocalNodeLeaderRevisionAfterElection() {
        LeaderElectionRecord record = null;
        try {
            record = readOnlyEndpointsLock.get(kubeApiClient);
        } catch (Exception e) {
            logger.warn("Could not read back leader data after being elected", e);
        }
        return createClusterMemberLeadershipObject(record, localMemberId);
    }

    private ClusterMembershipRevision<ClusterMemberLeadership> createClusterMemberLeadershipObject(LeaderElectionRecord record, String memberId) {
        ClusterMemberLeadership.Builder leadershipBuilder = ClusterMemberLeadership.newBuilder()
                .withMemberId(memberId)
                .withLeadershipState(ClusterMemberLeadershipState.Leader);

        long acquireTime;
        if (record != null) {
            acquireTime = record.getAcquireTime().toEpochSecond() * 1_000;

            Map<String, String> labels = new HashMap<>();
            labels.put("kube.elector.leaseDurationSeconds", "" + record.getLeaseDuration().getSeconds());
            labels.put("kube.elector.leaderTransitions", "" + record.getLeaderTransitions());
            labels.put("kube.elector.acquireTime", DateTimeExt.toUtcDateTimeString(record.getAcquireTime().toEpochSecond() * 1_000));
            labels.put("kube.elector.renewTime", DateTimeExt.toUtcDateTimeString(record.getRenewTime().toEpochSecond() * 1_000));
            leadershipBuilder.withLabels(labels);
        } else {
            acquireTime = titusRuntime.getClock().wallTime();
            leadershipBuilder.withLabels(Collections.emptyMap());
        }

        return ClusterMembershipRevision.<ClusterMemberLeadership>newBuilder()
                .withCurrent(leadershipBuilder.build())
                .withCode("elected")
                .withMessage("Leadership lock acquired in Kubernetes")
                .withRevision(acquireTime)
                .withTimestamp(acquireTime)
                .build();
    }

    private class LeaderElectionHandler {

        private final Thread leaderThread;

        private final Sinks.Many<ClusterMembershipEvent> leadershipStateProcessor = Sinks.many().replay().limit(1);

        private final AtomicBoolean leaderFlag = new AtomicBoolean();
        private final AtomicBoolean closed = new AtomicBoolean();

        private LeaderElectionHandler() {
            LeaderElector<?> leaderElector = kubeApiClient.leaderElector()
                    .withConfig(new LeaderElectionConfigBuilder()
                            .withName(clusterName)
                            .withLeaseDuration(leaseDuration)
                            .withLock(new LeaseLock(namespace, clusterName, localMemberId))
                            .withRenewDeadline(renewDeadline)
                            .withRetryPeriod(retryPeriod)
                            .withLeaderCallbacks(new LeaderCallbacks(
                                    () -> {
                                        logger.info("Local member elected a leader");
                                        processLeaderElectedCallback();
                                    },
                                    () -> {
                                        logger.info("Local member lost leadership");
                                        processLostLeadershipCallback();
                                    },
                                    newLeader -> logger.info("New leader elected: {}", newLeader)
                            )).build()
                    )
                    .build();

            this.leaderThread = new Thread("LeaderElectionHandler-" + LEADER_ELECTION_THREAD_IDX.getAndIncrement()) {
                @Override
                public void run() {
                    while (!closed.get()) {
                        long started = lastElectionAttemptAction.start();
                        try {
                            leaderElector.run();
                            if (leaderFlag.getAndSet(false)) {
                                processLostLeadershipCallback();
                            }
                            lastElectionAttemptAction.finish(started);
                        } catch (Throwable e) {
                            lastElectionAttemptAction.failure(started, e);
                            leaderFlag.set(false);
                            logger.info("Leader election attempt error: {}", e.getMessage());
                            logger.debug("Stack trace:", e);
                        }
                    }
                    leadershipStateProcessor.tryEmitComplete();
                    logger.info("Leaving {} thread", Thread.currentThread().getName());
                }
            };
            leaderThread.start();
        }

        private Flux<ClusterMembershipEvent> events() {
            return leadershipStateProcessor.asFlux();
        }

        private boolean isLeader() {
            return leaderFlag.get();
        }

        private boolean isDone() {
            return !leaderThread.isAlive();
        }

        private void leave() {
            closed.set(true);
            leaderThread.interrupt();
        }

        private void processLeaderElectedCallback() {
            leaderFlag.set(true);

            ClusterMembershipRevision<ClusterMemberLeadership> revision = createLocalNodeLeaderRevisionAfterElection();
            leadershipStateProcessor.tryEmitNext(ClusterMembershipEvent.leaderElected(revision));
        }

        private void processLostLeadershipCallback() {
            leaderFlag.set(false);

            ClusterMemberLeadership.Builder leadershipBuilder = ClusterMemberLeadership.newBuilder()
                    .withMemberId(localMemberId)
                    .withLeadershipState(ClusterMemberLeadershipState.NonLeader);

            ClusterMembershipRevision<ClusterMemberLeadership> revision = ClusterMembershipRevision.<ClusterMemberLeadership>newBuilder()
                    .withCurrent(leadershipBuilder.build())
                    .build();

            leadershipStateProcessor.tryEmitNext(ClusterMembershipEvent.leaderLost(revision));
            leadershipStateProcessor.tryEmitComplete();
        }
    }
}
