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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadership;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadershipState;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipEvent;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.DateTimeExt;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.extended.leaderelection.LeaderElectionConfig;
import io.kubernetes.client.extended.leaderelection.LeaderElectionRecord;
import io.kubernetes.client.extended.leaderelection.LeaderElector;
import io.kubernetes.client.extended.leaderelection.resourcelock.EndpointsLock;
import io.kubernetes.client.util.ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.ReplayProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

class DefaultK8LeaderElectionExecutor implements K8LeaderElectionExecutor {

    private static final Logger logger = LoggerFactory.getLogger(DefaultK8LeaderElectionExecutor.class);

    private static final AtomicInteger LEADER_ELECTION_THREAD_IDX = new AtomicInteger();
    private static final AtomicInteger WATCHER_THREAD_IDX = new AtomicInteger();

    private static final Duration LEADER_POLL_INTERVAL = Duration.ofSeconds(1);

    private final String namespace;
    private final String clusterName;
    private final String localMemberId;

    private final Duration leaseDuration;
    private final Duration retryPeriod;

    private final ApiClient k8ApiClient;
    private final TitusRuntime titusRuntime;

    private final EndpointsLock readOnlyEndpointsLock;

    private final AtomicReference<LeaderElectionHandler> leaderElectionHandlerRef = new AtomicReference<>();
    private final ReplayProcessor<LeaderElectionHandler> handlerProcessor = ReplayProcessor.create();

    DefaultK8LeaderElectionExecutor(ApiClient k8ApiClient,
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

        this.k8ApiClient = k8ApiClient;
        this.titusRuntime = titusRuntime;
        this.readOnlyEndpointsLock = new EndpointsLock(namespace, clusterName, localMemberId, k8ApiClient);
    }

    @Override
    public boolean isInLeaderElectionProcess() {
        LeaderElectionHandler handler = leaderElectionHandlerRef.get();
        return handler != null && !handler.isDone();
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

    @Override
    public Flux<ClusterMembershipEvent> watchLeaderElectionProcessUpdates() {
        Flux<ClusterMembershipEvent> lockWatcher = Flux.defer(() -> {
            Scheduler singleScheduler = Schedulers.newSingle("LeaderWatcher-" + WATCHER_THREAD_IDX.getAndIncrement());
            return Flux.interval(LEADER_POLL_INTERVAL, singleScheduler)
                    .flatMap(tick -> {
                        ClusterMembershipRevision<ClusterMemberLeadership> revision;
                        try {
                            revision = createCurrentLeaderRevision("N/A", true);
                        } catch (Exception e) {
                            return Flux.error(e.getCause() != null ? e.getCause() : e);
                        }
                        return Flux.<ClusterMembershipEvent>just(ClusterMembershipEvent.leaderElected(revision));
                    })
                    .doOnCancel(singleScheduler::dispose)
                    .doAfterTerminate(singleScheduler::dispose);
        }).distinctUntilChanged();

        return lockWatcher.mergeWith(handlerProcessor.flatMap(LeaderElectionHandler::events));
    }

    private ClusterMembershipRevision<ClusterMemberLeadership> createCurrentLeaderRevision(String memberId,
                                                                                           boolean failOnGetError) {
        LeaderElectionRecord record = null;
        String effectiveMemberId;
        try {
            record = readOnlyEndpointsLock.get();
            effectiveMemberId = record.getHolderIdentity();
        } catch (Exception e) {
            if (failOnGetError) {
                throw new IllegalStateException(e);
            }
            effectiveMemberId = memberId;
            logger.warn("Could not read back leader data after being elected", e);
        }

        ClusterMemberLeadership.Builder leadershipBuilder = ClusterMemberLeadership.newBuilder()
                .withMemberId(effectiveMemberId)
                .withLeadershipState(ClusterMemberLeadershipState.Leader);

        long acquireTime;
        if (record != null) {
            acquireTime = record.getAcquireTime().getTime();

            Map<String, String> labels = new HashMap<>();
            labels.put("k8.elector.leaseDurationSeconds", "" + record.getLeaseDurationSeconds());
            labels.put("k8.elector.leaderTransitions", "" + record.getLeaderTransitions());
            labels.put("k8.elector.acquireTime", DateTimeExt.toUtcDateTimeString(record.getAcquireTime().getTime()));
            labels.put("k8.elector.renewTime", DateTimeExt.toUtcDateTimeString(record.getRenewTime().getTime()));
            leadershipBuilder.withLabels(labels);
        } else {
            acquireTime = titusRuntime.getClock().wallTime();
            leadershipBuilder.withLabels(Collections.emptyMap());
        }

        return ClusterMembershipRevision.<ClusterMemberLeadership>newBuilder()
                .withCurrent(leadershipBuilder.build())
                .withCode("elected")
                .withMessage("Leadership lock acquired in K8S")
                .withRevision(acquireTime)
                .withTimestamp(acquireTime)
                .build();
    }

    private class LeaderElectionHandler {

        private final Thread leaderThread;

        private final FluxProcessor<ClusterMembershipEvent, ClusterMembershipEvent>
                leadershipStateProcessor = ReplayProcessor.<ClusterMembershipEvent>create(1).serialize();

        private final AtomicBoolean leaderFlag = new AtomicBoolean();

        private LeaderElectionHandler() {
            EndpointsLock lock = new EndpointsLock(namespace, clusterName, localMemberId, k8ApiClient);
            LeaderElectionConfig leaderElectionConfig = new LeaderElectionConfig(lock, leaseDuration, null, retryPeriod);
            LeaderElector leaderElector = new LeaderElector(leaderElectionConfig);

            this.leaderThread = new Thread("LeaderElectionHandler-" + LEADER_ELECTION_THREAD_IDX.getAndIncrement()) {
                @Override
                public void run() {
                    try {
                        leaderElector.run(
                                () -> processLeaderElectedCallback(),
                                () -> processLostLeadershipCallback()
                        );
                        if (leaderFlag.getAndSet(false)) {
                            processLostLeadershipCallback();
                        }
                        leadershipStateProcessor.onComplete();
                    } catch (Throwable e) {
                        leaderFlag.set(false);
                        leadershipStateProcessor.onError(e);
                    }
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
            leaderThread.interrupt();
        }

        private void processLeaderElectedCallback() {
            leaderFlag.set(true);

            ClusterMembershipRevision<ClusterMemberLeadership> revision = createCurrentLeaderRevision(localMemberId, false);
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

            leadershipStateProcessor.onNext(ClusterMembershipEvent.lostLeadership(revision));
            leadershipStateProcessor.onComplete();
        }
    }

    public static void main(String[] args) throws Exception {
        ApiClient client = ClientBuilder
                .standard()
                .setBasePath("http://100.65.82.159:7001")
                .build();
        client.getHttpClient().setReadTimeout(0, TimeUnit.SECONDS); // infinite timeout

        DefaultK8LeaderElectionExecutor executor = new DefaultK8LeaderElectionExecutor(
                client, "default", "titusrelocation-devtbakcell001", Duration.ofSeconds(10), "member1",
                TitusRuntimes.internal()
        );

        executor.watchLeaderElectionProcessUpdates().subscribe(
                event -> System.out.println("    Watcher: " + event),
                Throwable::printStackTrace,
                () -> System.out.println("    Watcher DONE")
        );

        for (int i = 0; i < 100; i++) {
            while (!executor.joinLeaderElectionProcess()) {
                System.out.println("...not ready yet");
                Thread.sleep(1000);
            }
            executor.leaveLeaderElectionProcess();
        }

        Thread.sleep(60_000);
    }
}
