package io.netflix.titus.master.agent.service.monitor;

import io.netflix.titus.api.agent.model.monitor.AgentStatus;

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
