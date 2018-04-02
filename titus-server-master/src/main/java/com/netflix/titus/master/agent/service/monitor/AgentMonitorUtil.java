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

import com.netflix.titus.api.agent.model.monitor.AgentStatus;

public class AgentMonitorUtil {

    /**
     * Compare two agent status to see if they describe the same state.
     */
    public static boolean equivalent(AgentStatus first, AgentStatus second) {
        if (first == second) {
            return true;
        }
        if (second == null || first.getClass() != second.getClass()) {
            return false;
        }
        if (first.getSourceId() != null ? !first.getSourceId().equals(second.getSourceId()) : second.getSourceId() != null) {
            return false;
        }
        if (first.getAgentInstance() != null ? !first.getAgentInstance().equals(second.getAgentInstance()) : second.getAgentInstance() != null) {
            return false;
        }
        if (first.getStatusCode() != second.getStatusCode()) {
            return false;
        }
        return true;
    }

    public static String toStatusUpdateSummary(AgentStatus agentStatus) {
        return String.format("[%s] Status update: %s -> %s (%s)",
                agentStatus.getSourceId(), agentStatus.getAgentInstance().getId(), agentStatus.getStatusCode(), agentStatus.getDescription()
        );
    }
}
