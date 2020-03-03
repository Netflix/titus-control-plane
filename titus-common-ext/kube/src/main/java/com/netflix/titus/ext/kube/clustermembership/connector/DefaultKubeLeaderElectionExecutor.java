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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.PreDestroy;

import com.google.common.annotations.VisibleForTesting;
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
import com.netflix.titus.common.util.spectator.ActionMetrics;
import com.netflix.titus.common.util.spectator.SpectatorExt;
import io.kubernetes.client.extended.leaderelection.LeaderElectionConfig;
import io.kubernetes.client.extended.leaderelection.LeaderElectionRecord;
import io.kubernetes.client.extended.leaderelection.LeaderElector;
import io.kubernetes.client.extended.leaderelection.Lock;
import io.kubernetes.client.extended.leaderelection.resourcelock.EndpointsLock;
import io.kubernetes.client.openapi.ApiClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.ReplayProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * The leader election process and the current leader discovery are handled separately. The former is handled by
 * Kubernetes {@link LeaderElector} object, which only tells when the local node becomes a leader. To get
 * information about who is the leader, we check periodically the associated {@link Lock}.
 */
class DefaultKubeLeaderElectionExecutor implements KubeLeaderElectionExecutor {

    private static final Logger logger = LoggerFactory.getLogger(DefaultKubeLeaderElectionExecutor.class);

    private static final AtomicInteger LEADER_ELECTION_THREAD_IDX = new AtomicInteger();
    private static final AtomicInteger WATCHER_THREAD_IDX = new AtomicInteger();

    private static final Duration LEADER_POLL_INTERVAL = Duration.ofSeconds(1);
    private static final int LEADER_POLL_RETRIES = 3;

    private final String namespace;
    private final String clusterName;
    private final String localMemberId;

    private final Duration leaseDuration;
    private final Duration retryPeriod;

    private final ApiClient kubeApiClient;
    private final TitusRuntime titusRuntime;

    private final EndpointsLock readOnlyEndpointsLock;

    private final AtomicReference<LeaderElectionHandler> leaderElectionHandlerRef = new AtomicReference<>();
    private final ReplayProcessor<LeaderElectionHandler> handlerProcessor = ReplayProcessor.create();

    // Metrics
    private final Registry registry;
    private final Id inLeaderElectionProcessMetricId;
    private final Id isLeaderMetricId;

    private final ActionMetrics lastElectionAttemptAction;
    private final ActionMetrics electedLeaderRefreshAction;

    DefaultKubeLeaderElectionExecutor(ApiClient kubeApiClient,
                                      String namespace,
                                      String clusterName,
                                      Duration leaseDuration,
                                      String localMemberId,
                                      TitusRuntime titusRuntime) {
        this.namespace = namespace;
        this.clusterName = clusterName;
        this.leaseDuration = leaseDuration;
        this.retryPeriod = leaseDuration.dividedBy(2);
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

        this.kubeApiClient = kubeApiClient;
        this.titusRuntime = titusRuntime;
        this.readOnlyEndpointsLock = new EndpointsLock(namespace, clusterName, localMemberId, kubeApiClient);
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

    @VisibleForTesting
    boolean isLeader() {
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
                    .retryBackoff(LEADER_POLL_RETRIES, Duration.ofMillis(Math.max(1, leaseDuration.toMillis() / 10)), leaseDuration)
                    .doOnCancel(singleScheduler::dispose)
                    .doAfterTerminate(singleScheduler::dispose);
        }).distinctUntilChanged();

        return lockWatcher.mergeWith(handlerProcessor.flatMap(LeaderElectionHandler::events));
    }

    private ClusterMembershipRevision<ClusterMemberLeadership> refreshCurrentLeaderRevision() {
        try {
            return createClusterMemberLeadershipObject(readOnlyEndpointsLock.get(), readOnlyEndpointsLock.get().getHolderIdentity());
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
            record = readOnlyEndpointsLock.get();
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
            acquireTime = record.getAcquireTime().getTime();

            Map<String, String> labels = new HashMap<>();
            labels.put("kube.elector.leaseDurationSeconds", "" + record.getLeaseDurationSeconds());
            labels.put("kube.elector.leaderTransitions", "" + record.getLeaderTransitions());
            labels.put("kube.elector.acquireTime", DateTimeExt.toUtcDateTimeString(record.getAcquireTime().getTime()));
            labels.put("kube.elector.renewTime", DateTimeExt.toUtcDateTimeString(record.getRenewTime().getTime()));
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

        private final FluxProcessor<ClusterMembershipEvent, ClusterMembershipEvent>
                leadershipStateProcessor = ReplayProcessor.<ClusterMembershipEvent>create(1).serialize();

        private final AtomicBoolean leaderFlag = new AtomicBoolean();
        private final AtomicBoolean closed = new AtomicBoolean();

        private LeaderElectionHandler() {
            EndpointsLock lock = new EndpointsLock(namespace, clusterName, localMemberId, kubeApiClient);
            LeaderElectionConfig leaderElectionConfig = new LeaderElectionConfig(lock, leaseDuration, null, retryPeriod);
            LeaderElector leaderElector = new LeaderElector(leaderElectionConfig);

            this.leaderThread = new Thread("LeaderElectionHandler-" + LEADER_ELECTION_THREAD_IDX.getAndIncrement()) {
                @Override
                public void run() {
                    while (!closed.get()) {
                        long started = lastElectionAttemptAction.start();
                        try {
                            leaderElector.run(
                                    () -> {
                                        logger.info("Local member elected a leader");
                                        processLeaderElectedCallback();
                                    },
                                    () -> {
                                        logger.info("Local member lost leadership");
                                        processLostLeadershipCallback();
                                    }
                            );
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
                    leadershipStateProcessor.onComplete();
                    logger.info("Leaving {} thread", Thread.currentThread().getName());
                }
            };
            leaderThread.start();
        }

        private Flux<ClusterMembershipEvent> events() {
            return leadershipStateProcessor;
        }

        private boolean isLeader() {
            return leaderFlag.get();
        }

        private boolean isDone() {
            return leadershipStateProcessor.isTerminated() && !leaderThread.isAlive();
        }

        private void leave() {
            closed.set(true);
            leaderThread.interrupt();
        }

        private void processLeaderElectedCallback() {
            leaderFlag.set(true);

            ClusterMembershipRevision<ClusterMemberLeadership> revision = createLocalNodeLeaderRevisionAfterElection();
            leadershipStateProcessor.onNext(ClusterMembershipEvent.leaderElected(revision));
        }

        private void processLostLeadershipCallback() {
            leaderFlag.set(false);

            ClusterMemberLeadership.Builder leadershipBuilder = ClusterMemberLeadership.newBuilder()
                    .withMemberId(localMemberId)
                    .withLeadershipState(ClusterMemberLeadershipState.NonLeader);

            ClusterMembershipRevision<ClusterMemberLeadership> revision = ClusterMembershipRevision.<ClusterMemberLeadership>newBuilder()
                    .withCurrent(leadershipBuilder.build())
                    .build();

            leadershipStateProcessor.onNext(ClusterMembershipEvent.leaderLost(revision));
            leadershipStateProcessor.onComplete();
        }
    }
}
