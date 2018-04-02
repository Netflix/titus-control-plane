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

package com.netflix.titus.api.agent.model.event;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstance;

public class AgentInstanceUpdateEvent extends AgentEvent {

    private final AgentInstance agentInstance;

    public AgentInstanceUpdateEvent(AgentInstance agentInstance) {
        this.agentInstance = agentInstance;
    }

    public AgentInstance getAgentInstance() {
        return agentInstance;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AgentInstanceUpdateEvent that = (AgentInstanceUpdateEvent) o;

        return agentInstance != null ? agentInstance.equals(that.agentInstance) : that.agentInstance == null;
    }

    @Override
    public int hashCode() {
        return agentInstance != null ? agentInstance.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "AgentInstanceUpdateEvent{" +
                "agentInstance=" + agentInstance +
                '}';
    }
}
