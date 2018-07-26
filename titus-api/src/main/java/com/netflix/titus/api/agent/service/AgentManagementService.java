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

package com.netflix.titus.api.agent.service;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.AutoScaleRule;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleStatus;
import com.netflix.titus.api.agent.model.InstanceOverrideStatus;
import com.netflix.titus.api.agent.model.event.AgentEvent;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.util.tuple.Either;
import com.netflix.titus.common.util.tuple.Pair;
import rx.Completable;
import rx.Observable;

public interface AgentManagementService {

    /**
     * Return all known agent instance groups.
     */
    List<AgentInstanceGroup> getInstanceGroups();

    /**
     * Get an agent instance group by id.
     *
     * @throws AgentManagementException {@link AgentManagementException.ErrorCode#InstanceGroupNotFound} if the instance group is not found
     */
    AgentInstanceGroup getInstanceGroup(String instanceGroupId);

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
    AgentInstance getAgentInstance(String instanceId);

    /**
     * Find an instance by id.
     */
    Optional<AgentInstance> findAgentInstance(String instanceId);

    /**
     * Find all agent instances matching a given filter.
     */
    List<Pair<AgentInstanceGroup, List<AgentInstance>>> findAgentInstances(Predicate<Pair<AgentInstanceGroup, AgentInstance>> filter);

    /**
     * For a given instance type, return the maximum amount of resources that can be allocated to a container.
     *
     * @throws AgentManagementException if the instance type is not known
     */
    ResourceDimension getResourceLimits(String instanceType);

    /**
     * For a given instance type, return the maximum amount of resources that can be allocated to a container or
     * {@link Optional#empty()} if the instance type is not known.
     */
    Optional<ResourceDimension> findResourceLimits(String instanceType);

    /**
     * Change tier assignment of an agent instance group.
     */
    Completable updateInstanceGroupTier(String instanceGroupId, Tier tier);

    /**
     * Associates a new auto scaling rule with a given instance group.
     *
     * @return AgentManagementException if the instance group is not found
     */
    Completable updateAutoScalingRule(String instanceGroupId, AutoScaleRule autoScaleRule);

    /**
     * Changes lifecycle status of a instance group.
     *
     * @return AgentManagementException if the instance group is not found
     */
    Completable updateInstanceGroupLifecycle(String instanceGroupId, InstanceGroupLifecycleStatus instanceGroupLifecycleStatus);

    /**
     * Changes attributes of an instance group.
     *
     * @return AgentManagementException if the instance group is not found
     */
    Completable updateInstanceGroupAttributes(String instanceGroupId, Map<String, String> attributes);

    /**
     * Updates instance group capacity. If only min value is provided, the desired size is adjusted to be no less than min.
     * If only desired value is provided, min value is adjusted to be no bigger than desired.
     * In any case, both min and desired cannot exceed max value, which cannot be changed via this API.
     *
     * @param instanceGroupId
     * @param min             if set, change the min size of the instance group
     * @param desired         if set, change the desired size of the instance group
     * @return AgentManagementException if the instance group is not found
     * @deprecated Use instead {@link #scaleUp(String, int)}.
     */
    @Deprecated
    Completable updateCapacity(String instanceGroupId, Optional<Integer> min, Optional<Integer> desired);

    /**
     * Increase instance group size by the given number of instances. The following constraints are checked prior
     * to admitting scale up:
     * <ul>
     * <li>scaleUpCount >= 0 (if scaleUpCount == 0, the operation is void)</li>
     * <li>desired + scaleUpCount <= max instance group size (throws an error if not)</li>
     * </ul>
     *
     * @param scaleUpCount number of instances to add (must be >= 0)
     * @return AgentManagementException if the instance group is not found
     */
    Completable scaleUp(String instanceGroupId, int scaleUpCount);

    /**
     * Add/change override status of the given agent server.
     *
     * @return AgentManagementException if the instance group is not found
     */
    Completable updateInstanceOverride(String instanceId, InstanceOverrideStatus instanceOverrideStatus);

    /**
     * Remove status override of the given agent server.
     *
     * @return AgentManagementException if the instance group is not found
     */
    Completable removeInstanceOverride(String instanceId);

    /**
     * Terminate agents with the given instance ids. The agents must belong to the same instance group.
     */
    Observable<List<Either<Boolean, Throwable>>> terminateAgents(String instanceGroupId, List<String> instanceIds, boolean shrink);

    /**
     * Refresh any cached state now.
     */
    void forceRefresh();

    /**
     * On subscription emit all known agent instance groups and instances, followed by a marker event. Next emit an
     * event for each instance group or agent instance change (add/update/remove).
     */
    Observable<AgentEvent> events(boolean includeSnapshot);
}
