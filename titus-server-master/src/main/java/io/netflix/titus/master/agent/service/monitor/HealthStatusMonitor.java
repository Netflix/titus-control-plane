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
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.spectator.api.Registry;
import io.netflix.titus.api.agent.model.AgentInstance;
import io.netflix.titus.api.agent.model.monitor.AgentStatus;
import io.netflix.titus.api.agent.service.AgentManagementFunctions;
import io.netflix.titus.api.agent.service.AgentManagementService;
import io.netflix.titus.api.agent.service.AgentStatusMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

/**
 * {@link AgentStatusMonitor} implementation depending on external health status provider. A client must
 * provide {@link AgentHealthResolver} implementation as a bridge to health source.
 */
@Singleton
public class HealthStatusMonitor implements AgentStatusMonitor {

    private static final Logger logger = LoggerFactory.getLogger(HealthStatusMonitor.class);

    static final String SOURCE_ID = "health";

    /**
     * Unhealthy agent should be disabled until it becomes healthy again, in which case it gets
     * enabled immediately. Since we need to provide the disable period, it is sent to a reasonable high value.
     * In case healtcheck service becomes unavailable, all unhealthy nodes will become available after this time.
     */
    static final long UNHEALTHY_NODE_DISABLE_TIME = 5 * 60 * 1000; // 5 minutes

    public interface AgentHealthResolver {
        boolean isHealthy(String agentId);
    }

    private final AgentManagementService agentManagementService;
    private final AgentHealthResolver healthResolver;
    private final AgentMonitorConfiguration config;
    private final AgentStatusMonitorMetrics metrics;
    private final Scheduler scheduler;

    public HealthStatusMonitor(AgentManagementService agentManagementService,
                               AgentHealthResolver healthResolver,
                               AgentMonitorConfiguration config,
                               Registry registry,
                               Scheduler scheduler) {
        this.agentManagementService = agentManagementService;
        this.healthResolver = healthResolver;
        this.config = config;
        this.metrics = new AgentStatusMonitorMetrics("healthStatusMonitor", registry);
        this.scheduler = scheduler;
    }

    @Inject
    public HealthStatusMonitor(AgentManagementService agentManagementService,
                               AgentHealthResolver healthResolver,
                               Registry registry,
                               AgentMonitorConfiguration config) {
        this(agentManagementService, healthResolver, config, registry, Schedulers.computation());
    }

    @Override
    public Observable<AgentStatus> monitor() {
        return Observable.interval(0, config.getHealthPollingInterval(), TimeUnit.MILLISECONDS, scheduler)
                .flatMap(tick -> {
                    try {
                        List<AgentStatus> statusList = resolveHealthStatusOfAllAgents();
                        metrics.replaceStatusChanges(statusList);
                        return Observable.from(statusList);
                    } catch (Exception e) {
                        logger.error("Health status evaluation error", e);
                        return Observable.empty();
                    }
                });
    }

    private List<AgentStatus> resolveHealthStatusOfAllAgents() {
        List<AgentInstance> agents = AgentManagementFunctions.getAllInstances(agentManagementService);
        List<AgentStatus> statusValues = new ArrayList<>(agents.size());
        agents.forEach(agent -> {
            String id = agent.getId();
            if (healthResolver.isHealthy(id)) {
                statusValues.add(AgentStatus.healthy(SOURCE_ID, agent));
                logger.debug("[{}] is healthy", id);
            } else {
                statusValues.add(AgentStatus.unhealthy(SOURCE_ID, agent, UNHEALTHY_NODE_DISABLE_TIME, scheduler.now()));
                logger.debug("[{}] is unhealthy", id);
            }
        });
        return statusValues;
    }
}
