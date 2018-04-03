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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.aws.AwsInstanceType;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class AgentDeployment {

    private final Map<String, AgentInstanceGroup> instanceGroups;
    private final Map<String, Map<String, AgentInstance>> instancesByGroup;

    private AgentDeployment(DeploymentBuilder deploymentBuilder) {
        this.instanceGroups = deploymentBuilder.instanceGroups;
        this.instancesByGroup = deploymentBuilder.instancesByGroup;
    }

    public AgentInstance getFirstInstance() {
        return instancesByGroup.values().stream().filter(v -> !v.isEmpty())
                .findFirst()
                .map(p -> p.values().iterator().next())
                .orElseThrow(() -> new IllegalStateException("No agent instances available"));
    }

    public List<AgentInstanceGroup> getInstanceGroups() {
        return new ArrayList<>(instanceGroups.values());
    }

    public AgentInstanceGroup getInstanceGroup(String instanceGroupId) {
        AgentInstanceGroup instanceGroup = instanceGroups.get(instanceGroupId);
        Preconditions.checkState(instanceGroup != null, "Requested unknown agent instance group");
        return instanceGroup;
    }

    public List<AgentInstance> getInstances(String instanceGroupId) {
        Map<String, AgentInstance> instanceMap = instancesByGroup.get(instanceGroupId);
        Preconditions.checkState(instanceMap != null, "Unknown agent instance group " + instanceGroupId);
        return new ArrayList<>(instanceMap.values());
    }

    public AgentInstance getInstance(String instanceId) {
        for (Map<String, AgentInstance> instanceMap : instancesByGroup.values()) {
            for (AgentInstance instance : instanceMap.values()) {
                if (instanceId.equals(instance.getId())) {
                    return instance;
                }
            }
        }
        throw new IllegalStateException("Unknown agent instance " + instanceId);
    }

    public List<AgentInstanceGroup> getInstanceGroupsOfType(String instanceType) {
        return instanceGroups.values().stream().filter(g -> g.getInstanceType().equals(instanceType)).collect(Collectors.toList());
    }

    public static AgentManagementService instrumentMock(AgentDeployment deployment, AgentManagementService agentManagementService) {
        when(agentManagementService.getInstanceGroups()).thenReturn(deployment.getInstanceGroups());
        when(agentManagementService.getInstanceGroup(anyString())).thenAnswer(argument -> deployment.getInstanceGroup(argument.getArgument(0)));
        when(agentManagementService.getAgentInstances(anyString())).thenAnswer(argument -> deployment.getInstances(argument.getArgument(0)));
        when(agentManagementService.getAgentInstance(anyString())).thenAnswer(argument -> deployment.getInstance(argument.getArgument(0)));
        return agentManagementService;
    }

    public static DeploymentBuilder newDeployment() {
        return new DeploymentBuilder();
    }

    public static class DeploymentBuilder {

        private final Map<String, AgentInstanceGroup> instanceGroups = new HashMap<>();
        private final Map<String, Map<String, AgentInstance>> instancesByGroup = new HashMap<>();

        public DeploymentBuilder withInstanceGroup(AgentInstanceGroup.Builder instanceGroupBuilder) {
            AgentInstanceGroup instanceGroup = instanceGroupBuilder.build();
            instanceGroups.put(instanceGroup.getId(), instanceGroup);
            return this;
        }

        public DeploymentBuilder withActiveInstanceGroup(Tier tier, String id, AwsInstanceType instanceType, int desired) {
            AgentInstanceGroup instanceGroup = AgentGenerator.agentServerGroups(tier, desired, instanceType).getValue().toBuilder()
                    .withId(id)
                    .build();

            instanceGroups.put(instanceGroup.getId(), instanceGroup);

            HashMap<String, AgentInstance> instanceMap = new HashMap<>();
            instancesByGroup.put(instanceGroup.getId(), instanceMap);
            AgentGenerator.agentInstances(instanceGroup).toList(desired).forEach(instance -> instanceMap.put(instance.getId(), instance));

            return this;
        }

        public AgentDeployment build() {
            return new AgentDeployment(this);
        }
    }
}
