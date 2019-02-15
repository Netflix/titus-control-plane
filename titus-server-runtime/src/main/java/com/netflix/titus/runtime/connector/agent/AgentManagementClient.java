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

package com.netflix.titus.runtime.connector.agent;

import java.util.List;
import java.util.Map;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.agent.model.event.AgentEvent;
import com.netflix.titus.api.model.Page;
import com.netflix.titus.api.model.PageResult;
import com.netflix.titus.api.model.Tier;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface AgentManagementClient {

    Mono<List<AgentInstanceGroup>> getInstanceGroups();

    Mono<AgentInstanceGroup> getInstanceGroup(String id);

    Mono<AgentInstance> getAgentInstance(String id);

    Mono<PageResult<AgentInstance>> findAgentInstances(Map<String, String> filteringCriteria, Page page);

    Mono<Void> updateInstanceGroupTier(String instanceGroupId, Tier tier);

    Mono<Void> updateInstanceGroupLifecycleState(String instanceGroupId, InstanceGroupLifecycleState lifecycleState);

    Mono<Void> updateInstanceGroupAttributes(String instanceGroupId, Map<String, String> attributes);

    Mono<Void> updateAgentInstanceAttributes(String instanceId, Map<String, String> attributes);

    Flux<AgentEvent> observeAgents();
}
