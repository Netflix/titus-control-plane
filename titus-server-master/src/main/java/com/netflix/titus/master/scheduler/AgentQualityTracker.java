package com.netflix.titus.master.scheduler;

public interface AgentQualityTracker {

    /**
     * Evaluates quality of an agent with the given id from 0 to 1 (0 == do not use it, 1 == perfect).
     *
     * @param agentHostName an agent host name
     * @return the agent's quality score from 0 to 1, or -1 if there is no agent with the given id
     */
    double qualityOf(String agentHostName);
}
