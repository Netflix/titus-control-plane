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

package com.netflix.titus.testkit.model.agent;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.event.AgentInstanceRemovedEvent;
import com.netflix.titus.api.agent.model.event.AgentInstanceUpdateEvent;
import com.netflix.titus.api.agent.model.monitor.AgentStatus;
import com.netflix.titus.api.agent.service.AgentStatusMonitor;
import rx.Observable;

class StubbedAgentStatusMonitor implements AgentStatusMonitor {

    private final StubbedAgentData stubbedAgentData;

    StubbedAgentStatusMonitor(StubbedAgentData stubbedAgentData) {
        this.stubbedAgentData = stubbedAgentData;
    }

    @Override
    public AgentStatus getStatus(String agentInstanceId) {
        AgentInstance instance = stubbedAgentData.getInstance(agentInstanceId);
        return AgentStatus.healthy("test", instance, "test", 0);
    }

    @Override
    public boolean isHealthy(String agentInstanceId) {
        return getStatus(agentInstanceId).getStatusCode() == AgentStatus.AgentStatusCode.Healthy;
    }

    @Override
    public Observable<AgentStatus> monitor() {
        return stubbedAgentData.observeAgents(false)
                .filter(event -> event instanceof AgentInstanceUpdateEvent || event instanceof AgentInstanceRemovedEvent)
                .map(event -> {
                    if (event instanceof AgentInstanceUpdateEvent) {
                        AgentInstanceUpdateEvent updateEvent = (AgentInstanceUpdateEvent) event;
                        return AgentStatus.healthy("test", updateEvent.getAgentInstance(), "test", 0);
                    }
                    AgentInstanceRemovedEvent removedEvent = (AgentInstanceRemovedEvent) event;
                    return AgentStatus.terminated("test", stubbedAgentData.getInstance(removedEvent.getAgentInstanceId()), "test", 0);
                });
    }
}
