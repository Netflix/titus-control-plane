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

package com.netflix.titus.master.agent.store;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.inject.Singleton;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.store.AgentStore;
import com.netflix.titus.common.util.rx.ObservableExt;
import rx.Completable;
import rx.Observable;

@Singleton
public class InMemoryAgentStore implements AgentStore {

    private final ConcurrentMap<String, AgentInstanceGroup> instanceGroupById = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, AgentInstance> instanceById = new ConcurrentHashMap<>();

    @Override
    public Observable<AgentInstanceGroup> retrieveAgentInstanceGroups() {
        return ObservableExt.fromCallable(instanceGroupById::values);
    }

    @Override
    public Observable<AgentInstance> retrieveAgentInstances() {
        return ObservableExt.fromCallable(instanceById::values);
    }

    @Override
    public Completable storeAgentInstanceGroup(AgentInstanceGroup agentInstanceGroup) {
        return Completable.fromAction(() -> instanceGroupById.put(agentInstanceGroup.getId(), agentInstanceGroup));
    }

    @Override
    public Completable storeAgentInstance(AgentInstance agentInstance) {
        return Completable.fromAction(() -> instanceById.put(agentInstance.getId(), agentInstance));
    }

    @Override
    public Completable removeAgentInstanceGroups(List<String> agentInstanceGroupIds) {
        return Completable.fromAction(() -> agentInstanceGroupIds.forEach(instanceGroupById::remove));
    }

    @Override
    public Completable removeAgentInstances(List<String> agentInstanceIds) {
        return Completable.fromAction(() -> agentInstanceIds.forEach(instanceById::remove));
    }
}
