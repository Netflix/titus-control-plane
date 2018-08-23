package com.netflix.titus.master.scheduler;

import com.netflix.titus.api.model.Tier;

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
         * The hard constraint name is stored in a {@link #failureReason} field.
         */
        JobHardConstraint,

        Unrecognized,
    }

    private final String taskId;
    private final FailureKind failureKind;
    private final String failureReason;
    private final Tier tier;

    /**
     * Number of agents for which this failure kind was found, or -1 if this value is not relevant.
     */
    private final int agentCount;

    public TaskPlacementFailure(String taskId, FailureKind failureKind, String failureReason, Tier tier, int agentCount) {
        this.taskId = taskId;
        this.failureKind = failureKind;
        this.failureReason = failureReason;
        this.tier = tier;
        this.agentCount = agentCount;
    }

    public String getTaskId() {
        return taskId;
    }

    public FailureKind getFailureKind() {
        return failureKind;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public Tier getTier() {
        return tier;
    }

    public int getAgentCount() {
        return agentCount;
    }
}
