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

package io.netflix.titus.api.agent.model.monitor;

import java.util.Collections;
import java.util.List;

import io.netflix.titus.api.agent.model.AgentInstance;

/**
 * An immutable object representing an agent's status together with the source of the status value
 * and the agent's id.
 */
public final class AgentStatus {

    public enum AgentStatusCode {Healthy, Unhealthy, Terminated}

    /**
     * A unique id of source providing status value.
     */
    private final String sourceId;

    /**
     * Agent instance for which health status is evaluated.
     */
    private final AgentInstance agentInstance;

    /**
     * Status code value reported by the give source for the given agent
     */
    private final AgentStatusCode statusCode;

    /**
     * Additional information about the current state. In case of unhealthy or unknown states, it should indicate the
     * root cause of it.
     */
    private final String description;

    /**
     * Time when agent status information was issued. The effective agent disable time must be compensated by the
     * elapsed time.
     */
    private final long emitTime;

    /**
     * For agent status evaluators aggregating other evaluators, provides the underlying results.
     */
    private final List<AgentStatus> components;

    private AgentStatus(String sourceId,
                        AgentInstance agentInstance,
                        AgentStatusCode statusCode,
                        String description,
                        long emitTime,
                        List<AgentStatus> components) {
        this.sourceId = sourceId;
        this.agentInstance = agentInstance;
        this.statusCode = statusCode;
        this.description = description;
        this.emitTime = emitTime;
        this.components = components;
    }

    public String getSourceId() {
        return sourceId;
    }

    public AgentInstance getAgentInstance() {
        return agentInstance;
    }

    public AgentStatusCode getStatusCode() {
        return statusCode;
    }

    public String getDescription() {
        return description;
    }

    public long getEmitTime() {
        return emitTime;
    }

    public List<AgentStatus> getComponents() {
        return components;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AgentStatus that = (AgentStatus) o;

        if (emitTime != that.emitTime) {
            return false;
        }
        if (sourceId != null ? !sourceId.equals(that.sourceId) : that.sourceId != null) {
            return false;
        }
        if (agentInstance != null ? !agentInstance.equals(that.agentInstance) : that.agentInstance != null) {
            return false;
        }
        if (statusCode != that.statusCode) {
            return false;
        }
        if (description != null ? !description.equals(that.description) : that.description != null) {
            return false;
        }
        return components != null ? components.equals(that.components) : that.components == null;
    }

    @Override
    public int hashCode() {
        int result = sourceId != null ? sourceId.hashCode() : 0;
        result = 31 * result + (agentInstance != null ? agentInstance.hashCode() : 0);
        result = 31 * result + (statusCode != null ? statusCode.hashCode() : 0);
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (int) (emitTime ^ (emitTime >>> 32));
        result = 31 * result + (components != null ? components.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "AgentStatus{" +
                "sourceId='" + sourceId + '\'' +
                ", agentInstance=" + agentInstance +
                ", statusCode=" + statusCode +
                ", description='" + description + '\'' +
                ", emitTime=" + emitTime +
                ", components=" + components +
                '}';
    }

    public static AgentStatus healthy(String sourceId, AgentInstance agentInstance, String description, long emitTime) {
        return new AgentStatus(sourceId, agentInstance, AgentStatusCode.Healthy, description, emitTime, Collections.emptyList());
    }

    public static AgentStatus healthy(String sourceId, AgentInstance agentInstance, String description, long emitTime, List<AgentStatus> components) {
        return new AgentStatus(sourceId, agentInstance, AgentStatusCode.Healthy, description, emitTime, components);
    }

    public static AgentStatus unhealthy(String sourceId, AgentInstance agentInstance, String description, long emitTime) {
        return new AgentStatus(sourceId, agentInstance, AgentStatusCode.Unhealthy, description, emitTime, Collections.emptyList());
    }

    public static AgentStatus unhealthy(String sourceId, AgentInstance agentInstance, String description, long emitTime, List<AgentStatus> components) {
        return new AgentStatus(sourceId, agentInstance, AgentStatusCode.Unhealthy, description, emitTime, components);
    }

    public static AgentStatus terminated(String sourceId, AgentInstance agentInstance, String description, long emitTime) {
        return new AgentStatus(sourceId, agentInstance, AgentStatusCode.Terminated, description, emitTime, Collections.emptyList());
    }

    public static AgentStatus terminated(String sourceId, AgentInstance agentInstance, String description, long emitTime, List<AgentStatus> components) {
        return new AgentStatus(sourceId, agentInstance, AgentStatusCode.Terminated, description, emitTime, components);
    }
}
