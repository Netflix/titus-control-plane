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

package com.netflix.titus.testkit.model.agent;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceLifecycleStatus;
import com.netflix.titus.api.agent.model.event.AgentEvent;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.agent.service.AgentStatusMonitor;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.util.rx.ReactorExt;
import reactor.core.publisher.Flux;

/**
 * A stub for the agent management subsystem to be used in unit tests. Provides means to create/update/delete agent test data.
 * The primary agent public APIs are stubbed, and bridged to the internal data store.
 */
public class AgentComponentStub {

    private final StubbedAgentData stubbedAgentData = new StubbedAgentData();

    private final AgentManagementService agentManagementService = new StubbedAgentManagementService(stubbedAgentData);

    private final AgentStatusMonitor agentStatusMonitor = new StubbedAgentStatusMonitor(stubbedAgentData);

    public AgentManagementService getAgentManagementService() {
        return agentManagementService;
    }

    public AgentStatusMonitor getAgentStatusMonitor() {
        return agentStatusMonitor;
    }

    public AgentInstance getFirstInstance() {
        return stubbedAgentData.getFirstInstance();
    }

    public Flux<AgentEvent> grpcObserveAgents(boolean snapshot) {
        return ReactorExt.toFlux(stubbedAgentData.observeAgents(snapshot));
    }

    public AgentComponentStub addInstanceGroup(AgentInstanceGroup instanceGroup) {
        stubbedAgentData.addInstanceGroup(instanceGroup);
        return this;
    }

    public AgentComponentStub removeInstanceGroup(String instanceGroupId) {
        stubbedAgentData.removeInstanceGroup(instanceGroupId);
        return this;
    }

    public AgentComponentStub terminateInstance(String instanceId, boolean shrink) {
        stubbedAgentData.terminateInstance(instanceId, shrink);
        return this;
    }

    public AgentComponentStub changeTier(String instanceGroupId, Tier tier) {
        stubbedAgentData.changeInstanceGroup(instanceGroupId, previous -> previous.toBuilder().withTier(tier).build());
        return this;
    }

    public AgentComponentStub changeInstanceLifecycleStatus(String instanceId, InstanceLifecycleStatus status) {
        stubbedAgentData.changeInstance(instanceId, previous -> previous.toBuilder().withDeploymentStatus(status).build());
        return this;
    }

    public static AgentComponentStub newAgentComponent() {
        return new AgentComponentStub();
    }
}
