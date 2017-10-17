/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.agent.service.vm;

import java.util.List;
import java.util.Set;

import io.netflix.titus.api.agent.model.AgentInstance;
import io.netflix.titus.api.agent.model.AgentInstanceGroup;
import rx.Completable;
import rx.Observable;

public interface AgentCache {
    List<AgentInstanceGroup> getInstanceGroups();

    AgentInstanceGroup getInstanceGroup(String instanceGroupId);

    Set<AgentInstance> getAgentInstances(String instanceGroupId);

    AgentInstance getAgentInstance(String instanceId);

    Completable updateInstanceGroupStore(AgentInstanceGroup instanceGroup);

    Completable updateInstanceGroupStoreAndSyncCloud(AgentInstanceGroup instanceGroup);

    Completable updateAgentInstanceStore(AgentInstance agentInstance);

    Completable removeInstances(String instanceGroupId, Set<String> agentInstanceIds);

    void forceRefresh();

    Observable<CacheUpdateEvent> events();
}
