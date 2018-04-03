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

package com.netflix.titus.api.agent.store;

import java.util.List;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import rx.Completable;
import rx.Observable;

public interface AgentStore {

    /**
     * Retrieve all agent instance groups.
     */
    Observable<AgentInstanceGroup> retrieveAgentInstanceGroups();

    /**
     * Retrieve all agent server instances.
     */
    Observable<AgentInstance> retrieveAgentInstances();

    /**
     * Persist (add or update) the given agent server group entity.
     */
    Completable storeAgentInstanceGroup(AgentInstanceGroup agentInstanceGroup);

    /**
     * Persist (add or update) the given agent instance entity.
     */
    Completable storeAgentInstance(AgentInstance agentInstance);

    /**
     * Remove agent instance group entities associated with the given ids.
     */
    Completable removeAgentInstanceGroups(List<String> agentInstanceGroupIds);

    /**
     * Remove agent instances associated with the given ids.
     */
    Completable removeAgentInstances(List<String> agentInstanceIds);
}
