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

import java.util.Collection;
import java.util.EnumMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.agent.model.monitor.AgentStatus;
import com.netflix.titus.api.agent.model.monitor.AgentStatus.AgentStatusCode;
import com.netflix.titus.master.MetricConstants;

public class AgentStatusMonitorMetrics {

    private final String rootName;
    private final Registry registry;

    private volatile ConcurrentMap<String, AgentStatus> statusByAgent = new ConcurrentHashMap<>();
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
        if (agentStatus.getStatusCode() == AgentStatusCode.Terminated) {
            if (statusByAgent.remove(agentStatus.getAgentInstance().getId()) != null) {
                refreshStatusMap();
            }
        } else {
            statusByAgent.put(agentStatus.getAgentInstance().getId(), agentStatus);
            refreshStatusMap();
        }
    }

    /**
     * Swap all stored information with the provided one.
     */
    public void replaceStatusChanges(Collection<AgentStatus> statusList) {
        ConcurrentMap<String, AgentStatus> newStatusByAgent = new ConcurrentHashMap<>();
        statusList.forEach(status -> newStatusByAgent.put(status.getAgentInstance().getId(), status));
        this.statusByAgent = newStatusByAgent;
        refreshStatusMap();
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
        for (AgentStatus status : statusByAgent.values()) {
            AgentStatusCode statusCode = status.getStatusCode();
            newStatusMap.put(statusCode, newStatusMap.get(statusCode) + 1);
        }
        this.statusMap = newStatusMap;
    }

    private Id newId(String name) {
        return registry.createId(rootName + name);
    }
}
