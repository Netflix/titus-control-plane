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

import java.util.EnumMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import io.netflix.titus.api.agent.model.monitor.AgentStatus;
import io.netflix.titus.master.MetricConstants;
import io.netflix.titus.api.agent.model.monitor.AgentStatus.AgentStatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AgentStatusMonitorMetrics {

    private static final Logger logger = LoggerFactory.getLogger(AgentStatusMonitorMetrics.class);

    private final String rootName;
    private final Registry registry;

    private volatile ConcurrentMap<String, Optional<AgentStatus>> statusByAgent = new ConcurrentHashMap<>();
    private volatile EnumMap<AgentStatusCode, Integer> statusMap = createEmptyStatusMap();

    public AgentStatusMonitorMetrics(String name, Registry registry) {
        this.rootName = MetricConstants.METRIC_AGENT_MONITOR + name + '.';
        this.registry = registry;

        registry.gauge(newId("agentCount"), this, self -> self.statusByAgent.size());
        for (AgentStatusCode status : AgentStatusCode.values()) {
            registry.gauge(newId("status").withTag("status", status.name()), status, s -> statusMap.get(status));
        }
    }

    public void statusChanged(AgentStatus agentStatus) {
        if (statusByAgent.put(agentStatus.getInstance().getId(), Optional.of(agentStatus)) == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Agent {} is not tracked by the metrics monitor", agentStatus.getInstance().getId());
            }
        }
        refreshStatusMap();
    }

    /**
     * Swap all stored information with the provided one.
     */
    public void replaceStatusChanges(List<AgentStatus> statusList) {
        ConcurrentMap<String, Optional<AgentStatus>> newStatusByAgent = new ConcurrentHashMap<>();
        statusList.forEach(status -> newStatusByAgent.put(status.getInstance().getId(), Optional.of(status)));
        this.statusByAgent = newStatusByAgent;
        refreshStatusMap();
    }

    public void agentAdded(String agentName) {
        if (statusByAgent.put(agentName, Optional.empty()) != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Agent {} is already tracked by the metrics monitor", agentName);
            }
        }
    }

    public void agentRemoved(String agentName) {
        if (statusByAgent.remove(agentName) == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Agent {} is not tracked by the metrics monitor", agentName);
            }
        }
    }

    private EnumMap<AgentStatusCode, Integer> createEmptyStatusMap() {
        EnumMap<AgentStatusCode, Integer> newStatusMap = new EnumMap<>(AgentStatusCode.class);
        for (AgentStatusCode status : AgentStatusCode.values()) {
            newStatusMap.put(status, 0);
        }
        return newStatusMap;
    }

    private void refreshStatusMap() {
        EnumMap<AgentStatusCode, Integer> newStatusMap = createEmptyStatusMap();
        for (Optional<AgentStatus> status : statusByAgent.values()) {
            if (status.isPresent()) {
                AgentStatusCode statusCode = status.get().getStatusCode();
                newStatusMap.put(statusCode, newStatusMap.get(statusCode) + 1);
            }
        }
        this.statusMap = newStatusMap;
    }

    private Id newId(String name) {
        return registry
                .createId(rootName + name)
                .withTag("class", AgentStatusMonitorMetrics.class.getSimpleName());
    }
}
