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

public class AgentInstanceRemovedEvent extends AgentEvent {

    private final String agentInstanceId;

    public AgentInstanceRemovedEvent(String agentInstanceId) {
        this.agentInstanceId = agentInstanceId;
    }

    public String getAgentInstanceId() {
        return agentInstanceId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AgentInstanceRemovedEvent that = (AgentInstanceRemovedEvent) o;

        return agentInstanceId != null ? agentInstanceId.equals(that.agentInstanceId) : that.agentInstanceId == null;
    }

    @Override
    public int hashCode() {
        return agentInstanceId != null ? agentInstanceId.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "AgentInstanceRemovedEvent{" +
                "agentInstanceId='" + agentInstanceId + '\'' +
                '}';
    }
}
