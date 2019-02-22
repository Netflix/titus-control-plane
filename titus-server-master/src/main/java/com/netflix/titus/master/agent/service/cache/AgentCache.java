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

package com.netflix.titus.master.agent.service.cache;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import rx.Completable;
import rx.Observable;
import rx.Single;

public interface AgentCache {
    List<AgentInstanceGroup> getInstanceGroups();

    AgentInstanceGroup getInstanceGroup(String instanceGroupId);

    Optional<AgentInstanceGroup> findInstanceGroup(String instanceGroupId);

    Set<AgentInstance> getAgentInstances(String instanceGroupId);

    AgentInstance getAgentInstance(String instanceId);

    Optional<AgentInstance> findAgentInstance(String instanceId);

    Completable updateInstanceGroupStore(AgentInstanceGroup instanceGroup);

    Completable updateInstanceGroupStoreAndSyncCloud(AgentInstanceGroup instanceGroup);

    Completable updateAgentInstanceStore(AgentInstance agentInstance);

    Single<AgentInstanceGroup> getAndUpdateInstanceGroupStore(String instanceGroupId, Function<AgentInstanceGroup, AgentInstanceGroup> function);

    Single<AgentInstanceGroup> getAndUpdateInstanceGroupStoreAndSyncCloud(String instanceGroupId, Function<AgentInstanceGroup, AgentInstanceGroup> function);

    Single<AgentInstance> getAndUpdateAgentInstanceStore(String instanceId, Function<AgentInstance, AgentInstance> function);

    Completable removeInstances(String instanceGroupId, Set<String> agentInstanceIds);

    void forceRefresh();

    Observable<CacheUpdateEvent> events();
}
