/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.master.scheduler;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.master.clusteroperations.ClusterAgentAutoScaler;

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
         * Kube API not ready.
         */
        KubeApiNotReady,

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
         * Task not launched on an agent, as it runs the maximum allowed number of containers.
         */
        AgentContainerLimit,

        /**
         * Task not launched due to job hard constraint. It has lower priority than the previous failure kinds.
         */
        JobHardConstraint,

        /**
         * Task not launched due to assigned IP allocation in use by another running task.
         */
        WaitingForInUseIpAllocation,

        /**
         * Task not launched due to failure to assign requested opportunistic resources
         */
        OpportunisticResource,

        Unrecognized;

        /**
         * Failures that are expected to go away, and should not be acted upon
         */
        public static final Set<FailureKind> TRANSIENT = Sets.immutableEnumSet(WaitingForInUseIpAllocation, LaunchGuard);

        /**
         * <tt>TRANSIENT</tt> and <tt>NoActiveAgent</tt> (all agents are non-schedulable for a task) must never modify
         * opportunistic scheduling behavior.
         *
         * @see DefaultSchedulingService
         */
        public static final Set<FailureKind> IGNORED_FOR_OPPORTUNISTIC_SCHEDULING = ImmutableSet.<FailureKind>builder()
                .addAll(TRANSIENT)
                .add(AgentContainerLimit)
                .add(NoActiveAgents)
                .build();

        /**
         * Failures that should never trigger cluster autoscaling
         *
         * @see ClusterAgentAutoScaler
         */
        public static final Set<FailureKind> NEVER_TRIGGER_AUTOSCALING = Sets.immutableEnumSet(WaitingForInUseIpAllocation, OpportunisticResource);
    }

    private final String taskId;
    private final FailureKind failureKind;
    private final Tier tier;

    /**
     * Number of agents for which this failure kind was found, or -1 if this value is not relevant.
     */
    private final int agentCount;

    /**
     * The original task placement result.
     */
    private final Map<String, Object> rawData;

    public TaskPlacementFailure(String taskId, FailureKind failureKind, int agentCount, Tier tier, Map<String, Object> rawData) {
        this.taskId = taskId;
        this.failureKind = failureKind;
        this.agentCount = agentCount;
        this.tier = tier;
        this.rawData = rawData;
    }

    public String getTaskId() {
        return taskId;
    }

    public FailureKind getFailureKind() {
        return failureKind;
    }

    public Tier getTier() {
        return tier;
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
                tier == that.tier &&
                Objects.equals(rawData, that.rawData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId, failureKind, tier, agentCount, rawData);
    }

    /**
     * Do not include raw data in the string representation.
     */
    @Override
    public String toString() {
        return "TaskPlacementFailure{" +
                "taskId='" + taskId + '\'' +
                ", failureKind=" + failureKind +
                ", tier=" + tier +
                ", agentCount=" + agentCount +
                ", rawData=" + rawData +
                '}';
    }
}
