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

package com.netflix.titus.runtime.clustermembership.activation;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.annotation.PreDestroy;

import com.google.common.base.Stopwatch;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.api.clustermembership.service.ClusterMembershipService;
import com.netflix.titus.api.common.LeaderActivationListener;
import com.netflix.titus.common.framework.scheduler.ScheduleReference;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.DateTimeExt;
import com.netflix.titus.common.util.retry.Retryers;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.spectator.SpectatorExt;
import com.netflix.titus.common.util.time.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * Coordinates the activation and deactivation processes for an elected leader.
 */
public class LeaderActivationCoordinator {

    private static final Logger logger = LoggerFactory.getLogger(LeaderActivationCoordinator.class);

    private static final String NAME = LeaderActivationListener.class.getSimpleName();

    private static final String LABEL_ROOT = "titus.leaderActivationCoordinator.";
    private static final String LABEL_STATE = LABEL_ROOT + "state";
    private static final String LABEL_TRANSITION_TIME = LABEL_ROOT + "transitionTime";

    private static final String METRIC_ROOT = "titus.clusterMembership.activation.";

    private static final ScheduleDescriptor SCHEDULE_DESCRIPTOR = ScheduleDescriptor.newBuilder()
            .withName(NAME)
            .withDescription("Leader activation/deactivation coordinator")
            .withInitialDelay(Duration.ZERO)
            .withInterval(Duration.ofMillis(200))
            .withTimeout(Duration.ofSeconds(60))
            .withRetryerSupplier(Retryers::never)
            .withOnErrorHandler((action, error) -> logger.error("Unexpected error", error))
            .build();

    public enum State {
        Awaiting,
        ElectedLeader,
        Deactivated
    }

    private final LeaderActivationConfiguration configuration;
    private final List<LeaderActivationListener> services;
    private final Consumer<Throwable> deactivationCallback;
    private final ClusterMembershipService membershipService;
    private final String localMemberId;

    private final AtomicReference<State> stateRef = new AtomicReference<>(State.Awaiting);
    private volatile List<LeaderActivationListener> activatedServices = Collections.emptyList();
    private volatile long activationTimestamp = -1;

    private final Scheduler scheduler;
    private final ScheduleReference scheduleRef;

    private final Clock clock;
    private final Registry registry;

    private final SpectatorExt.FsmMetrics<State> stateMetricFsm;
    private final Id inActiveStateTimeId;

    /**
     * @param services list of services to activate in the desired activation order
     */
    public LeaderActivationCoordinator(LeaderActivationConfiguration configuration,
                                       List<LeaderActivationListener> services,
                                       Consumer<Throwable> deactivationCallback,
                                       ClusterMembershipService membershipService,
                                       TitusRuntime titusRuntime) {
        this.configuration = configuration;
        this.services = services;
        this.deactivationCallback = deactivationCallback;
        this.membershipService = membershipService;
        this.localMemberId = membershipService.getLocalClusterMember().getCurrent().getMemberId();
        this.clock = titusRuntime.getClock();

        this.registry = titusRuntime.getRegistry();
        this.stateMetricFsm = SpectatorExt.fsmMetrics(
                registry.createId(METRIC_ROOT + "state"),
                s -> s == State.Deactivated,
                State.Awaiting,
                registry
        );
        this.inActiveStateTimeId = registry.createId(METRIC_ROOT + "inActiveStateTime");
        PolledMeter.using(registry).withId(inActiveStateTimeId).monitorValue(this, self ->
                (self.stateRef.get() == State.ElectedLeader && activationTimestamp > 0) ? self.clock.wallTime() - self.activationTimestamp : 0
        );

        // We create the thread here, as the default one is NonBlocking, and we allow React blocking subscriptions in
        // the activation phase.
        this.scheduler = Schedulers.newSingle(r -> {
            Thread thread = new Thread(r, "LeaderActivationCoordinator");
            thread.setDaemon(true);
            return thread;
        });
        this.scheduleRef = titusRuntime.getLocalScheduler().scheduleMono(
                SCHEDULE_DESCRIPTOR.toBuilder()
                        .withInterval(Duration.ofMillis(configuration.getLeaderCheckIntervalMs()))
                        .withTimeout(Duration.ofMillis(configuration.getLeaderActivationTimeout()))
                        .build(),
                context -> refresh(),
                scheduler
        );
    }

    private Mono<Void> refresh() {
        return Mono.defer(() -> {
            try {
                if (isLocalLeader()) {
                    activate();
                } else {
                    if (stateRef.get() != State.Awaiting) {
                        try {
                            deactivate();
                        } finally {
                            deactivationCallback.accept(new RuntimeException("Lost leadership"));
                        }
                    }
                }
                return recordState();
            } catch (Exception e) {
                return Mono.error(new IllegalStateException("Unexpected leader election coordinator error: " + e.getMessage(), e));
            }
        });
    }

    @PreDestroy
    public void shutdown() {
        activationTimestamp = -1;
        PolledMeter.remove(registry, inActiveStateTimeId);

        scheduleRef.cancel();

        // Give it some time to finish pending work if any.
        long start = clock.wallTime();
        while (!scheduleRef.isClosed() && !clock.isPast(start + configuration.getLeaderActivationTimeout())) {
            try {
                Thread.sleep(1_000);
            } catch (InterruptedException e) {
                break;
            }
        }
        scheduler.dispose();

        try {
            deactivate();
        } catch (Exception e) {
            deactivationCallback.accept(new RuntimeException(NAME + " component shutdown"));
        }
        recordState().subscribe(ReactorExt.silentSubscriber());
    }

    private void activate() {
        if (!stateRef.compareAndSet(State.Awaiting, State.ElectedLeader)) {
            logger.debug("Activation process has been already attempted. Component is in: state={}", stateRef.get());
            return;
        }

        logger.info("Starting the leader activation process (activating {} services)...", services.size());

        Stopwatch allStart = Stopwatch.createStarted();

        List<LeaderActivationListener> activated = new ArrayList<>();
        for (LeaderActivationListener service : services) {

            String serviceName = service.getClass().getSimpleName();
            logger.info("Activating service {}...", serviceName);

            Stopwatch serviceStart = Stopwatch.createStarted();
            try {
                service.activate();
                activated.add(service);

                logger.info("Service {} started in {}", serviceName, serviceStart.elapsed(TimeUnit.MILLISECONDS));
            } catch (Exception e) {
                logger.error("Service activation failure. Rolling back", e);
                this.activatedServices = activated;
                try {
                    deactivate();
                } finally {
                    deactivationCallback.accept(e);
                }
                return;
            }
        }
        this.activatedServices = activated;
        this.activationTimestamp = clock.wallTime();

        logger.info("Activation process finished in: {}", DateTimeExt.toTimeUnitString(allStart.elapsed(TimeUnit.MILLISECONDS)));
    }

    private void deactivate() {
        this.activationTimestamp = -1;

        State current = stateRef.getAndSet(State.Deactivated);
        if (current == State.Awaiting) {
            logger.info("Deactivating non-leader member");
            return;
        }
        if (current == State.Deactivated) {
            logger.debug("Already deactivated");
            return;
        }

        logger.info("Stopping the elected leader (deactivating {} services)...", activatedServices.size());
        Stopwatch allStart = Stopwatch.createStarted();

        List<LeaderActivationListener> deactivationList = new ArrayList<>(activatedServices);
        Collections.reverse(deactivationList);
        activatedServices.clear();

        for (LeaderActivationListener service : deactivationList) {
            String serviceName = service.getClass().getSimpleName();
            logger.info("Deactivating service {}...", serviceName);

            Stopwatch serviceStart = Stopwatch.createStarted();
            try {
                service.deactivate();
                logger.info("Service {} stopped in {}", serviceName, serviceStart.elapsed(TimeUnit.MILLISECONDS));
            } catch (Exception e) {
                logger.error("Failed to deactivate service: {}", serviceName);
            }
        }

        logger.info("Deactivation process finished in: {}", DateTimeExt.toTimeUnitString(allStart.elapsed(TimeUnit.MILLISECONDS)));
    }

    private Mono<Void> recordState() {
        stateMetricFsm.transition(stateRef.get());

        String recordedState = membershipService.getLocalClusterMember().getCurrent().getLabels().get(LABEL_STATE);
        if (stateRef.get().name().equals(recordedState)) {
            return Mono.empty();
        }
        return membershipService.updateSelf(current -> {
                    Map<String, String> labels = CollectionsExt.asMap(
                            LABEL_STATE, stateRef.get().name(),
                            LABEL_TRANSITION_TIME, DateTimeExt.toLocalDateTimeString(clock.wallTime())
                    );
                    return ClusterMembershipRevision.<ClusterMember>newBuilder()
                            .withCurrent(current.toBuilder()
                                    .withLabels(CollectionsExt.merge(current.getLabels(), labels))
                                    .build()
                            )
                            .build();
                }
        ).ignoreElement().cast(Void.class).onErrorResume(error -> {
            logger.info("Failed to append {} labels", NAME, error);
            return Mono.empty();
        });
    }

    private boolean isLocalLeader() {
        return membershipService.findLeader()
                .map(leader -> leader.getCurrent().getMemberId().equals(localMemberId))
                .orElse(false);
    }
}
