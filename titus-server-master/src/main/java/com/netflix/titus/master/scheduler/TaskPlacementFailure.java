package com.netflix.titus.master.scheduler;

import java.util.Map;
import java.util.Objects;

public class TaskPlacementFailure {

    public enum FailureKind {
        /**
         * There are zero active agents in a tier.
         */
        NoActiveAgents,

        /**
         * All agents are fully allocated to running containers.
         */
        AllAgentsFull,

        /**
         * Task in a capacity group, that reached its capacity limit.
         */
        AboveCapacityLimit,

        /**
         * Task does not fit into any available agent.
         */
        TooLargeToFit,

        /**
         * Task not launched yet due to launch guard lock on one or more agents.
         * If an agent with a LaunchGuard state is found, all other failures associated with this task are ignored.
         */
        LaunchGuard,

        /**
         * Task not launched due to job hard constraint. It has lower priority than the previous failure kinds.
         */
        JobHardConstraint,

        Unrecognized,
    }

    private final String taskId;
    private final FailureKind failureKind;

    /**
     * Number of agents for which this failure kind was found, or -1 if this value is not relevant.
     */
    private final int agentCount;

    /**
     * The original task placement result.
     */
    private final Map<String, Object> rawData;

    public TaskPlacementFailure(String taskId, FailureKind failureKind, int agentCount, Map<String, Object> rawData) {
        this.taskId = taskId;
        this.failureKind = failureKind;
        this.agentCount = agentCount;
        this.rawData = rawData;
    }

    public String getTaskId() {
        return taskId;
    }

    public FailureKind getFailureKind() {
        return failureKind;
    }

    public int getAgentCount() {
        return agentCount;
    }

    public Map<String, Object> getRawData() {
        return rawData;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TaskPlacementFailure that = (TaskPlacementFailure) o;
        return agentCount == that.agentCount &&
                Objects.equals(taskId, that.taskId) &&
                failureKind == that.failureKind &&
                Objects.equals(rawData, that.rawData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId, failureKind, agentCount, rawData);
    }

    /**
     * Do not include raw data in the string representation.
     */
    @Override
    public String toString() {
        return "TaskPlacementFailure{" +
                "taskId='" + taskId + '\'' +
                ", failureKind=" + failureKind +
                ", agentCount=" + agentCount +
                '}';
    }
}
