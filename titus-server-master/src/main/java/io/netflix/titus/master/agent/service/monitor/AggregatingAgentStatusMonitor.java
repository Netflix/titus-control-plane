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
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.spectator.api.Registry;
import io.netflix.titus.api.agent.model.monitor.AgentStatus;
import io.netflix.titus.api.agent.model.monitor.AgentStatus.AgentStatusCode;
import io.netflix.titus.api.agent.service.AgentManagementException;
import io.netflix.titus.api.agent.service.AgentManagementFunctions;
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
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;

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

    private static final String SOURCE_ID = "aggregatingStatusMonitor";

    static long INITIAL_RETRY_INTERVAL_MS = 1_000;
    static long MAX_RETRY_INTERVAL_MS = 10_000;

    private final Registry registry;
    private final Scheduler scheduler;

    private final AgentManagementService agentManagementService;
    private final AgentStatusMonitorMetrics metrics;
    private final Set<AgentStatusMonitor> delegates;

    private final PublishSubject<AgentStatus> statusUpdateSubject = PublishSubject.create();

    private Subscription downstreamUpdatesSubscription;

    public AggregatingAgentStatusMonitor(Set<AgentStatusMonitor> delegates,
                                         AgentManagementService agentManagementService,
                                         Registry registry,
                                         Scheduler scheduler) {
        this.delegates = delegates;
        this.agentManagementService = agentManagementService;
        this.metrics = new AgentStatusMonitorMetrics(SOURCE_ID, registry);
        this.registry = registry;
        this.scheduler = scheduler;
    }

    @Inject
    public AggregatingAgentStatusMonitor(Set<AgentStatusMonitor> delegates,
                                         AgentManagementService agentManagementService,
                                         Registry registry) {
        this(delegates, agentManagementService, registry, Schedulers.computation());
    }

    @Activator
    public void enterActiveMode() {
        RetryHandlerBuilder retryTemplate = RetryHandlerBuilder.retryHandler()
                .withUnlimitedRetries()
                .withDelay(INITIAL_RETRY_INTERVAL_MS, MAX_RETRY_INTERVAL_MS, TimeUnit.MILLISECONDS)
                .withScheduler(scheduler);

        Observable<AgentStatus> downstreamUpdates = Observable.merge(merge(delegates))
                .compose(subscriptionMetrics(METRIC_AGENT_MONITOR + "monitorSubscription", AggregatingAgentStatusMonitor.class, registry))
                .retryWhen(
                        retryTemplate.but().withTitle("agent status monitor").buildExponentialBackoff()
                );
        this.downstreamUpdatesSubscription = downstreamUpdates
                .compose(ObservableExt.head(this::getAllStatuses))
                .subscribe(update -> {
                    AgentStatus aggregatedStatus;
                    try {
                        try {
                            aggregatedStatus = getStatusInternal(update.getAgentInstance().getId());
                        } catch (AgentManagementException e) {
                            if (e.getErrorCode() != AgentManagementException.ErrorCode.AgentNotFound) {
                                throw e;
                            }
                            aggregatedStatus = AgentStatus.terminated(SOURCE_ID, update.getAgentInstance(), "Terminated", scheduler.now());
                        }

                        logger.info(AgentMonitorUtil.toStatusUpdateSummary(aggregatedStatus));
                        metrics.statusChanged(aggregatedStatus);
                        statusUpdateSubject.onNext(aggregatedStatus);
                    } catch (Exception e) {
                        logger.warn("Agent status update error", e);
                    }
                });
    }

    @PreDestroy
    public void shutdown() {
        ObservableExt.safeUnsubscribe(downstreamUpdatesSubscription);
    }

    @Override
    public AgentStatus getStatus(String agentInstanceId) {
        return getStatusInternal(agentInstanceId);
    }

    @Override
    public Observable<AgentStatus> monitor() {
        return statusUpdateSubject.asObservable();
    }

    // Public method is guarded by activation framework, and we need to call it before it completes
    private AgentStatus getStatusInternal(String agentInstanceId) {
        List<AgentStatus> statuses = delegates.stream()
                .map(d -> d.getStatus(agentInstanceId))
                .collect(Collectors.toList());

        AgentStatus unhealthy = null;
        for (AgentStatus status : statuses) {
            if (status.getStatusCode() == AgentStatusCode.Terminated) {
                return AgentStatus.terminated(status.getSourceId(), status.getAgentInstance(), "Agent terminated", status.getEmitTime(), statuses);
            }
            if (status.getStatusCode() == AgentStatusCode.Unhealthy) {
                unhealthy = status;
            }
        }
        if (unhealthy != null) {
            return AgentStatus.unhealthy(unhealthy.getSourceId(), unhealthy.getAgentInstance(), unhealthy.getSourceId() + " returned unhealthy status", unhealthy.getEmitTime(), statuses);
        }

        AgentStatus first = statuses.get(0);
        return AgentStatus.healthy(first.getSourceId(), first.getAgentInstance(), "All downstream monitors return status healthy", scheduler.now(), statuses);
    }

    private List<AgentStatus> getAllStatuses() {
        List<AgentStatus> result = new ArrayList<>();
        AgentManagementFunctions.getAllInstances(agentManagementService).forEach(instance -> {
            try {
                result.add(getStatusInternal(instance.getId()));
            } catch (Exception ignore) {
                logger.warn("Expected agent instance {} not found: {}", instance.getId(), ignore.getMessage());
            }
        });
        return result;
    }

    private List<Observable<AgentStatus>> merge(Set<AgentStatusMonitor> delegates) {
        List<Observable<AgentStatus>> sourceObservables = new ArrayList<>(delegates.size());
        delegates.forEach(d -> sourceObservables.add(d.monitor()));
        return sourceObservables;
    }
}
