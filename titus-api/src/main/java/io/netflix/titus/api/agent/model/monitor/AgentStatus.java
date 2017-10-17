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

import io.netflix.titus.api.agent.model.AgentInstance;

/**
 * An immutable object representing an agent's status together with the source of the status value
 * and the agent's id.
 */
public final class AgentStatus {

    public enum AgentStatusCode {Healthy, Unhealthy}

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
     * Amount of time in milliseconds the node should be taken out of service if in unhealthy state.
     */
    private final long disableTime;

    /**
     * Time when agent status information was issued. The effective agent disable time must be compensated by the
     * elapsed time.
     */
    private final long emitTime;

    private AgentStatus(String sourceId, AgentInstance agentInstance, AgentStatusCode statusCode, long disableTime, long emitTime) {
        this.sourceId = sourceId;
        this.agentInstance = agentInstance;
        this.statusCode = statusCode;
        this.disableTime = disableTime;
        this.emitTime = emitTime;
    }

    public String getSourceId() {
        return sourceId;
    }

    public AgentInstance getInstance() {
        return agentInstance;
    }

    public AgentStatusCode getStatusCode() {
        return statusCode;
    }

    public long getDisableTime() {
        return disableTime;
    }

    public long getEmitTime() {
        return emitTime;
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

        if (disableTime != that.disableTime) {
            return false;
        }
        if (emitTime != that.emitTime) {
            return false;
        }
        if (sourceId != null ? !sourceId.equals(that.sourceId) : that.sourceId != null) {
            return false;
        }
        if (agentInstance != null ? !agentInstance.equals(that.agentInstance) : that.agentInstance != null) {
            return false;
        }
        return statusCode == that.statusCode;
    }

    @Override
    public int hashCode() {
        int result = sourceId != null ? sourceId.hashCode() : 0;
        result = 31 * result + (agentInstance != null ? agentInstance.hashCode() : 0);
        result = 31 * result + (statusCode != null ? statusCode.hashCode() : 0);
        result = 31 * result + (int) (disableTime ^ (disableTime >>> 32));
        result = 31 * result + (int) (emitTime ^ (emitTime >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "AgentStatus{" +
                "sourceId='" + sourceId + '\'' +
                ", agentInstance=" + agentInstance +
                ", statusCode=" + statusCode +
                ", disableTime=" + disableTime +
                ", emitTime=" + emitTime +
                '}';
    }

    public static AgentStatus healthy(String sourceId, AgentInstance agentInstance) {
        return new AgentStatus(sourceId, agentInstance, AgentStatusCode.Healthy, -1, -1);
    }

    public static AgentStatus healthy(AgentStatus status) {
        if (status.getStatusCode() == AgentStatusCode.Healthy) {
            return status;
        }
        return healthy(status.getSourceId(), status.getInstance());
    }

    public static AgentStatus unhealthy(String sourceId, AgentInstance agentInstance, long disableTime, long emitTime) {
        return new AgentStatus(sourceId, agentInstance, AgentStatusCode.Unhealthy, disableTime, emitTime);
    }
}
