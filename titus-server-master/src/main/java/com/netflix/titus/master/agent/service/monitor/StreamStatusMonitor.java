/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.titus.master.agent.service.monitor;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.monitor.AgentStatus;
import com.netflix.titus.api.agent.service.AgentManagementException;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.agent.service.AgentStatusMonitor;
import com.netflix.titus.common.util.rx.RetryHandlerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.subjects.PublishSubject;

/**
 * Helper {@link AgentStatusMonitor} implementation for handling streamed status updates.
 */
public class StreamStatusMonitor implements AgentStatusMonitor {

    private static final Logger logger = LoggerFactory.getLogger(StreamStatusMonitor.class);

    private static final long RETRY_DELAYS_MS = 1_000;

    private final String source;
    private final boolean failOnMissingData;
    private final Scheduler scheduler;

    private final AgentStatusMonitorMetrics metrics;
    private final Subscription statusUpdateSubscription;

    private final PublishSubject<AgentStatus> statusUpdateSubject = PublishSubject.create();
    private final AgentManagementService agentManagementService;
    private volatile ConcurrentMap<String, AgentStatus> instanceStatuses = new ConcurrentHashMap<>();

    public StreamStatusMonitor(String source,
                               boolean failOnMissingData,
                               AgentManagementService agentManagementService,
                               Observable<AgentStatus> agentStatusObservable,
                               Registry registry,
                               Scheduler scheduler) {
        this.source = source;
        this.failOnMissingData = failOnMissingData;
        this.agentManagementService = agentManagementService;
        this.scheduler = scheduler;
        this.metrics = new AgentStatusMonitorMetrics(source, registry);

        this.statusUpdateSubscription = agentStatusObservable
                .retryWhen(RetryHandlerBuilder.retryHandler()
                        .withUnlimitedRetries()
                        .withDelay(RETRY_DELAYS_MS, RETRY_DELAYS_MS, TimeUnit.MILLISECONDS)
                        .withScheduler(scheduler)
                        .buildExponentialBackoff()
                ).subscribe(this::handleStatusUpdate);
    }

    public void shutdown() {
        statusUpdateSubscription.unsubscribe();
    }

    @Override
    public AgentStatus getStatus(String agentInstanceId) {
        AgentStatus agentStatus = instanceStatuses.get(agentInstanceId);
        if (agentStatus == null) {
            if (failOnMissingData) {
                throw AgentManagementException.agentNotFound(agentInstanceId);
            }
            AgentInstance agentInstance = agentManagementService.getAgentInstance(agentInstanceId);
            return AgentStatus.healthy(source, agentInstance, "No data recorded yet; assuming healthy", scheduler.now());
        }
        return agentStatus;
    }

    @Override
    public Observable<AgentStatus> monitor() {
        return statusUpdateSubject.asObservable();
    }

    private void handleStatusUpdate(AgentStatus statusUpdate) {
        if (statusUpdate.getStatusCode() == AgentStatus.AgentStatusCode.Terminated) {
            instanceStatuses.remove(statusUpdate.getAgentInstance().getId());
        } else {
            instanceStatuses.put(statusUpdate.getAgentInstance().getId(), statusUpdate);
        }
        metrics.statusChanged(statusUpdate);
        logger.info("Status update: {} -> {} (source {})", statusUpdate.getAgentInstance(), statusUpdate.getStatusCode(), statusUpdate.getSourceId());
        statusUpdateSubject.onNext(statusUpdate);
    }
}
