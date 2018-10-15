package com.netflix.titus.api.agent.service;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.common.util.tuple.Pair;

public interface ReadOnlyAgentOperations {
    /**
     * Return all known agent instance groups.
     */
    List<AgentInstanceGroup> getInstanceGroups();

    /**
     * Get an agent instance group by id.
     *
     * @throws AgentManagementException {@link AgentManagementException.ErrorCode#InstanceGroupNotFound} if the instance group is not found
     */
    default AgentInstanceGroup getInstanceGroup(String instanceGroupId) {
        return findInstanceGroup(instanceGroupId).orElseThrow(() -> AgentManagementException.agentGroupNotFound(instanceGroupId));
    }

    /**
     * Find an instance group by id.
     */
    Optional<AgentInstanceGroup> findInstanceGroup(String instanceGroupId);

    /**
     * Get all agents belonging to the given instance group.
     *
     * @throws AgentManagementException {@link AgentManagementException.ErrorCode#InstanceGroupNotFound} if the instance group is not found
     */
    List<AgentInstance> getAgentInstances(String instanceGroupId);

    /**
     * Get an agent instance by id.
     *
     * @throws AgentManagementException {@link AgentManagementException.ErrorCode#AgentNotFound} if the agent instance is not found
     */
    default AgentInstance getAgentInstance(String instanceId) {
        return findAgentInstance(instanceId).orElseThrow(() -> AgentManagementException.agentNotFound(instanceId));
    }

    /**
     * Find an instance by id.
     */
    Optional<AgentInstance> findAgentInstance(String instanceId);

    /**
     * Find all agent instances matching a given filter.
     */
    List<Pair<AgentInstanceGroup, List<AgentInstance>>> findAgentInstances(Predicate<Pair<AgentInstanceGroup, AgentInstance>> filter);
}
