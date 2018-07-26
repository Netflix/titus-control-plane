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

package com.netflix.titus.master.scheduler.constraint;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.netflix.fenzo.ConstraintEvaluator.Result;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.queues.QAttributes;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleStatus;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.agent.service.AgentStatusMonitor;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;
import org.apache.mesos.Protos;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AgentManagementConstraintTest {

    private final SchedulerConfiguration schedulerConfiguration = mock(SchedulerConfiguration.class);
    private final AgentManagementService agentManagementService = mock(AgentManagementService.class);
    private final AgentStatusMonitor agentStatusMonitor = mock(AgentStatusMonitor.class);

    private final AgentManagementConstraint agentManagementConstraint = new AgentManagementConstraint(schedulerConfiguration, agentManagementService, agentStatusMonitor);

    @Before
    public void setUp() throws Exception {
        when(schedulerConfiguration.getInstanceGroupAttributeName()).thenReturn("asg");
        when(schedulerConfiguration.getInstanceAttributeName()).thenReturn("id");
    }

    @Test
    public void noInstanceGroupId() {
        Result result = agentManagementConstraint.evaluate(createTaskRequest(),
                createVirtualMachineCurrentStateMock("1234", ""), mock(TaskTrackerState.class));
        assertThat(result.isSuccessful()).isFalse();
        assertThat(result.getFailureReason()).isEqualToIgnoringCase("Missing instance group attribute");
    }

    @Test
    public void instanceGroupNotFound() {
        Result result = agentManagementConstraint.evaluate(createTaskRequest(),
                createVirtualMachineCurrentStateMock("", "instanceGroupId"), mock(TaskTrackerState.class));
        assertThat(result.isSuccessful()).isFalse();
        assertThat(result.getFailureReason()).isEqualToIgnoringCase("Instance group not found");
    }

    @Test
    public void instanceGroupNotActive() {
        AgentInstanceGroup agentInstanceGroup = createAgentInstanceGroup(InstanceGroupLifecycleState.Inactive, Tier.Flex);
        when(agentManagementService.findInstanceGroup("instanceGroupId")).thenReturn(Optional.of(agentInstanceGroup));
        Result result = agentManagementConstraint.evaluate(createTaskRequest(),
                createVirtualMachineCurrentStateMock("1234", "instanceGroupId"), mock(TaskTrackerState.class));
        assertThat(result.isSuccessful()).isFalse();
        assertThat(result.getFailureReason()).isEqualToIgnoringCase("Instance group is not active or phased out");
    }

    @Test
    public void instanceGroupTierMismatch() {
        AgentInstanceGroup agentInstanceGroup = createAgentInstanceGroup(InstanceGroupLifecycleState.Active, Tier.Critical);
        when(agentManagementService.findInstanceGroup("instanceGroupId")).thenReturn(Optional.of(agentInstanceGroup));
        Result result = agentManagementConstraint.evaluate(createTaskRequest(),
                createVirtualMachineCurrentStateMock("1234", "instanceGroupId"), mock(TaskTrackerState.class));
        assertThat(result.isSuccessful()).isFalse();
        assertThat(result.getFailureReason()).isEqualToIgnoringCase("Task cannot run on instance group tier");
    }

    @Test
    public void instanceGroupDoesNotHaveGpus() {
        AgentInstanceGroup agentInstanceGroup = createAgentInstanceGroup(InstanceGroupLifecycleState.Active, Tier.Flex);
        when(agentManagementService.findInstanceGroup("instanceGroupId")).thenReturn(Optional.of(agentInstanceGroup));
        TaskRequest taskRequest = createTaskRequest();
        HashMap<String, Double> scalars = new HashMap<>();
        scalars.put("gpu", 1.0);
        when(taskRequest.getScalarRequests()).thenReturn(scalars);
        Result result = agentManagementConstraint.evaluate(taskRequest,
                createVirtualMachineCurrentStateMock("1234", "instanceGroupId"), mock(TaskTrackerState.class));
        assertThat(result.isSuccessful()).isFalse();
        assertThat(result.getFailureReason()).isEqualToIgnoringCase("Instance group does not have gpus");
    }

    @Test
    public void instanceOnlyRunsGpuTasks() {
        AgentInstanceGroup agentInstanceGroup = createAgentInstanceGroup(InstanceGroupLifecycleState.Active, Tier.Flex, 1);
        when(agentManagementService.findInstanceGroup("instanceGroupId")).thenReturn(Optional.of(agentInstanceGroup));
        TaskRequest taskRequest = createTaskRequest();
        HashMap<String, Double> scalars = new HashMap<>();
        when(taskRequest.getScalarRequests()).thenReturn(scalars);
        Result result = agentManagementConstraint.evaluate(taskRequest,
                createVirtualMachineCurrentStateMock("1234", "instanceGroupId"), mock(TaskTrackerState.class));
        assertThat(result.isSuccessful()).isFalse();
        assertThat(result.getFailureReason()).isEqualToIgnoringCase("Instance group does not run non gpu tasks");
    }

    private VirtualMachineCurrentState createVirtualMachineCurrentStateMock(String id, String asg) {
        VirtualMachineCurrentState currentState = mock(VirtualMachineCurrentState.class);
        VirtualMachineLease lease = mock(VirtualMachineLease.class);
        Map<String, Protos.Attribute> attributes = new HashMap<>();
        attributes.put("id", Protos.Attribute.newBuilder().setName("id").setType(Protos.Value.Type.TEXT).setText(Protos.Value.Text.newBuilder().setValue(id)).build());
        attributes.put("asg", Protos.Attribute.newBuilder().setName("asg").setType(Protos.Value.Type.TEXT).setText(Protos.Value.Text.newBuilder().setValue(asg)).build());
        when(lease.getAttributeMap()).thenReturn(attributes);
        when(currentState.getCurrAvailableResources()).thenReturn(lease);
        return currentState;
    }

    private TaskRequest createTaskRequest() {
        V3QueueableTask taskRequest = mock(V3QueueableTask.class);
        QAttributes qAttributes = mock(QAttributes.class);
        when(qAttributes.getTierNumber()).thenReturn(1);
        when(taskRequest.getQAttributes()).thenReturn(qAttributes);
        return taskRequest;
    }

    private AgentInstanceGroup createAgentInstanceGroup(InstanceGroupLifecycleState state, Tier tier) {
        return createAgentInstanceGroup(state, tier, 0);
    }

    private AgentInstanceGroup createAgentInstanceGroup(InstanceGroupLifecycleState state, Tier tier, int gpus) {
        ResourceDimension resourceDimension = ResourceDimension.newBuilder()
                .withCpus(1)
                .withMemoryMB(4096)
                .withDiskMB(10000)
                .withNetworkMbs(128)
                .withGpu(gpus)
                .build();
        return AgentInstanceGroup.newBuilder()
                .withId("1234")
                .withResourceDimension(resourceDimension)
                .withLifecycleStatus(InstanceGroupLifecycleStatus.newBuilder().withState(state).build())
                .withTier(tier)
                .withTimestamp(System.currentTimeMillis())
                .build();
    }
}