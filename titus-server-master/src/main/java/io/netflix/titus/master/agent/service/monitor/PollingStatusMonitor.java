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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.PreDestroy;

import com.netflix.spectator.api.Registry;
import io.netflix.titus.api.agent.model.AgentInstance;
import io.netflix.titus.api.agent.model.monitor.AgentStatus;
import io.netflix.titus.api.agent.service.AgentManagementFunctions;
import io.netflix.titus.api.agent.service.AgentManagementService;
import io.netflix.titus.api.agent.service.AgentStatusMonitor;
import io.netflix.titus.common.util.CollectionsExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.subjects.PublishSubject;

/**
 * Helper {@link AgentStatusMonitor} implementation which periodically polls a health source.
 */
public abstract class PollingStatusMonitor implements AgentStatusMonitor {

    private static final Logger logger = LoggerFactory.getLogger(PollingStatusMonitor.class);

    private final String source;
    private final AgentManagementService agentManagementService;
    private final Scheduler scheduler;
    private final AgentStatusMonitorMetrics metrics;
    private final Subscription subscription;

    private final PublishSubject<AgentStatus> statusUpdateSubject = PublishSubject.create();
    private volatile Map<String, AgentStatus> instanceStatuses = new HashMap<>();

    protected PollingStatusMonitor(String source,
                                   AgentManagementService agentManagementService,
                                   AgentMonitorConfiguration config,
                                   Registry registry,
                                   Scheduler scheduler) {
        this.source = source;
        this.agentManagementService = agentManagementService;
        this.scheduler = scheduler;
        this.metrics = new AgentStatusMonitorMetrics(source, registry);
        this.subscription = Observable.interval(0, config.getHealthPollingInterval(), TimeUnit.MILLISECONDS, scheduler)
                .subscribe(tick -> {
                    try {
                        Map<String, AgentStatus> previousStatuses = this.instanceStatuses;
                        this.instanceStatuses = resolveHealthStatusOfAllAgents();
                        emitUpdates(previousStatuses, instanceStatuses);
                        metrics.replaceStatusChanges(this.instanceStatuses.values());
                    } catch (Exception e) {
                        logger.error("Health status evaluation error", e);
                    }
                });
    }

    @PreDestroy
    public void shutdown() {
        subscription.unsubscribe();
    }

    @Override
    public AgentStatus getStatus(String agentInstanceId) {
        return resolve(agentManagementService.getAgentInstance(agentInstanceId));
    }

    @Override
    public Observable<AgentStatus> monitor() {
        return statusUpdateSubject.asObservable();
    }

    protected abstract AgentStatus resolve(AgentInstance agentInstance);

    private Map<String, AgentStatus> resolveHealthStatusOfAllAgents() {
        List<AgentInstance> agents = AgentManagementFunctions.getAllInstances(agentManagementService);
        Map<String, AgentStatus> newInstanceStatuses = new HashMap<>();
        agents.forEach(agent -> {
            String id = agent.getId();
            AgentStatus agentStatus = resolve(agent);
            logger.debug("[{}] resolved status: ", id, agentStatus);
            newInstanceStatuses.put(id, agentStatus);
        });
        return newInstanceStatuses;
    }

    private void emitUpdates(Map<String, AgentStatus> previous, Map<String, AgentStatus> current) {
        Set<String> removed = CollectionsExt.copyAndRemove(previous.keySet(), current.keySet());
        removed.forEach(id -> {
                    AgentStatus terminated = AgentStatus.terminated(source, previous.get(id).getAgentInstance(), "Not found.", scheduler.now());
                    logger.info(AgentMonitorUtil.toStatusUpdateSummary(terminated));
                    statusUpdateSubject.onNext(terminated);
                }
        );
        current.forEach((id, agentStatus) -> {
            AgentStatus previousStatus = previous.get(agentStatus.getAgentInstance().getId());
            if (previousStatus == null || !AgentMonitorUtil.equivalent(previousStatus, agentStatus)) {
                logger.info(AgentMonitorUtil.toStatusUpdateSummary(agentStatus));
                statusUpdateSubject.onNext(agentStatus);
            }
        });
    }
}
