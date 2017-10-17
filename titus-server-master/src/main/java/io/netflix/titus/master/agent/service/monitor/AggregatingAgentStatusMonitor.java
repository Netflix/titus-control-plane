/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.agent.service.monitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.spectator.api.Registry;
import io.netflix.titus.api.agent.model.event.AgentInstanceRemovedEvent;
import io.netflix.titus.api.agent.model.monitor.AgentStatus;
import io.netflix.titus.api.agent.model.monitor.AgentStatus.AgentStatusCode;
import io.netflix.titus.api.agent.service.AgentManagementService;
import io.netflix.titus.api.agent.service.AgentStatusMonitor;
import io.netflix.titus.common.util.guice.annotation.Activator;
import io.netflix.titus.common.util.guice.annotation.ProxyConfiguration;
import io.netflix.titus.common.util.rx.ObservableExt;
import io.netflix.titus.common.util.rx.RetryHandlerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

import static io.netflix.titus.api.agent.model.monitor.AgentStatusExt.evaluateEffectiveStatus;
import static io.netflix.titus.api.agent.model.monitor.AgentStatusExt.isDifferent;
import static io.netflix.titus.api.agent.model.monitor.AgentStatusExt.isExpired;
import static io.netflix.titus.common.util.guice.ProxyType.ActiveGuard;
import static io.netflix.titus.common.util.guice.ProxyType.Logging;
import static io.netflix.titus.common.util.guice.ProxyType.Spectator;
import static io.netflix.titus.common.util.spectator.SpectatorExt.subscriptionMetrics;
import static io.netflix.titus.master.MetricConstants.METRIC_AGENT_MONITOR;

/**
 * {@link AggregatingAgentStatusMonitor} aggregates multiple status sources. Resulting status is
 * {@code Healthy} only if all statuses are {@code Healthy} (logical 'and').
 */
@Singleton
@ProxyConfiguration(types = {Logging, Spectator, ActiveGuard})
public class AggregatingAgentStatusMonitor implements AgentStatusMonitor {

    private static final Logger logger = LoggerFactory.getLogger(AggregatingAgentStatusMonitor.class);

    /**
     * A marker state, that replaces timeout exception for dead agents.
     */
    private static final AgentStatus TIMED_OUT_MARKER = AgentStatus.healthy(null, null);
    private static final Observable<AgentStatus> TIMED_OUT_OBSERVER = Observable.just(TIMED_OUT_MARKER);

    static long INITIAL_RETRY_INTERVAL_MS = 1_000;
    static long MAX_RETRY_INTERVAL_MS = 10_000;

    private final Observable<AgentStatus> aggregatedStatus;
    private final AgentMonitorConfiguration config;
    private final AgentManagementService agentManagementService;
    private final Registry registry;
    private final AgentStatusMonitorMetrics metrics;
    private final Scheduler scheduler;

    private final Func1<Observable<? extends Throwable>, Observable<?>> retryHandler;

    private final ConcurrentMap<String, AgentStatus> lastKnownStatusByAgentId = new ConcurrentHashMap<>();

    private Subscription lastKnownStatusSubscription;
    private Subscription agentEventStreamSubscription;

    public AggregatingAgentStatusMonitor(Set<AgentStatusMonitor> delegates,
                                         AgentMonitorConfiguration config,
                                         AgentManagementService agentManagementService,
                                         Registry registry,
                                         Scheduler scheduler) {
        this.config = config;
        this.agentManagementService = agentManagementService;
        this.registry = registry;
        this.metrics = new AgentStatusMonitorMetrics("aggregatingStatusMonitor", registry);
        this.scheduler = scheduler;
        this.retryHandler = failures -> failures.flatMap(f -> {
                    logger.warn("Agent status monitor error", f);
                    return Observable.timer(MAX_RETRY_INTERVAL_MS, TimeUnit.MILLISECONDS, scheduler);
                }
        );
        this.aggregatedStatus = order(merge(delegates)).share();
    }

    @Inject
    public AggregatingAgentStatusMonitor(Set<AgentStatusMonitor> delegates,
                                         AgentMonitorConfiguration config,
                                         AgentManagementService agentManagementService,
                                         Registry registry) {
        this(delegates, config, agentManagementService, registry, Schedulers.computation());
    }

    @Activator
    public void enterActiveMode() {
        RetryHandlerBuilder retryTemplate = RetryHandlerBuilder.retryHandler()
                .withUnlimitedRetries()
                .withDelay(INITIAL_RETRY_INTERVAL_MS, MAX_RETRY_INTERVAL_MS, TimeUnit.MILLISECONDS)
                .withScheduler(scheduler);

        this.lastKnownStatusSubscription = aggregatedStatus
                .compose(subscriptionMetrics(METRIC_AGENT_MONITOR + "monitorSubscription", AggregatingAgentStatusMonitor.class, registry))
                .retryWhen(
                        retryTemplate.but().withTitle("agent status monitor").buildExponentialBackoff()
                ).subscribe(agentStatus -> lastKnownStatusByAgentId.put(agentStatus.getInstance().getId(), agentStatus));

        this.agentEventStreamSubscription = agentManagementService.events(false)
                .compose(subscriptionMetrics(METRIC_AGENT_MONITOR + "agentEventsSubscription", AggregatingAgentStatusMonitor.class, registry))
                .retryWhen(
                        retryTemplate.but().withTitle("agent update monitor").buildExponentialBackoff()
                ).subscribe(event -> {
                    if (event instanceof AgentInstanceRemovedEvent) {
                        lastKnownStatusByAgentId.remove(((AgentInstanceRemovedEvent) event).getAgentInstanceId());
                    }
                });
    }

    @PreDestroy
    public void shutdown() {
        ObservableExt.safeUnsubscribe(lastKnownStatusSubscription, agentEventStreamSubscription);
    }

    @Override
    public Optional<AgentStatus> getCurrent(String agentInstanceId) {
        return Optional.ofNullable(lastKnownStatusByAgentId.get(agentInstanceId));
    }

    @Override
    public Observable<AgentStatus> monitor() {
        return aggregatedStatus.doOnNext(metrics::statusChanged);
    }

    protected void agentDisconnected(String hostname) {
        logger.info("Lack of activity detected from agent {}; disconnecting", hostname);
    }

    private List<Observable<AgentStatus>> merge(Set<AgentStatusMonitor> delegates) {
        List<Observable<AgentStatus>> sourceObservables = new ArrayList<>(delegates.size());
        delegates.forEach(d -> sourceObservables.add(d.monitor().retryWhen(retryHandler)));
        return sourceObservables;
    }

    private Observable<AgentStatus> order(List<Observable<AgentStatus>> sourceObservables) {
        return Observable.merge(sourceObservables)
                .groupBy(status -> status.getInstance().getId())
                .flatMap(agentGroupedObservable -> {
                            String agentId = agentGroupedObservable.getKey();
                            return mergeAgentStatusUpdates(
                                    agentId,
                                    agentGroupedObservable.timeout(config.getDeadAgentTimeout(), TimeUnit.MILLISECONDS, TIMED_OUT_OBSERVER, scheduler)
                            )
                                    .doOnSubscribe(() -> metrics.agentAdded(agentId))
                                    .doOnUnsubscribe(() -> {
                                        metrics.agentRemoved(agentId);
                                        agentDisconnected(agentId);
                                    });
                        }
                );
    }

    private Observable<AgentStatus> mergeAgentStatusUpdates(String agentId, Observable<AgentStatus> agentStatusUpdates) {
        return Observable.unsafeCreate(subscriber -> {
            Map<String, AgentStatus> statusBySource = new HashMap<>();
            AtomicReference<AgentStatus> lastEmitted = new AtomicReference<>();
            agentStatusUpdates.subscribe(
                    next -> {
                        if (next == TIMED_OUT_MARKER) {
                            logger.info("Agent {} status stream closed due to long inactivity", agentId);
                            return;
                        }
                        statusBySource.put(next.getSourceId(), next);
                        AgentStatus newStatus = evaluateEffectiveStatus(statusBySource.values(), scheduler);
                        if (isExpired(newStatus, scheduler)) {
                            AgentStatus forceOk = AgentStatus.healthy(newStatus);
                            subscriber.onNext(forceOk);
                            lastEmitted.set(forceOk);
                            logger.info("[{}->{}] effective new status is forced healthy (update={}, effective={})", agentId, AgentStatusCode.Healthy, next, newStatus);
                        } else {
                            if (isDifferent(newStatus, lastEmitted.get(), scheduler)) {
                                subscriber.onNext(newStatus);
                                logger.info("[{}->{}] changing agent state (update={}, effective={})", agentId, next.getStatusCode(), next, newStatus);
                            } else {
                                logger.debug("[{}->{}] effective new identical to the previous value (update={}, effective={})", agentId, next.getStatusCode(), next, newStatus);
                            }
                            lastEmitted.set(newStatus);
                        }
                    },
                    e -> {
                        logger.error("Agent {} status stream terminated with an error", agentId, e);
                        subscriber.onCompleted();
                    },
                    () -> {
                        logger.info("Agent {} status monitoring onCompleted", agentId);
                        subscriber.onCompleted();
                    }
            );
        });
    }
}
