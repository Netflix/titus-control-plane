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

package com.netflix.titus.master.scheduler;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.netflix.fenzo.AssignableVirtualMachine;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.agent.service.AgentStatusMonitor;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.model.Tier;
import org.junit.Before;
import org.junit.Test;

import static com.netflix.titus.master.scheduler.SchedulerTestUtils.INSTANCE_GROUP_ID;
import static com.netflix.titus.master.scheduler.SchedulerTestUtils.INSTANCE_ID;
import static com.netflix.titus.master.scheduler.SchedulerTestUtils.TASK_ID;
import static com.netflix.titus.master.scheduler.SchedulerTestUtils.UNKNOWN_INSTANCE_GROUP_ID;
import static com.netflix.titus.master.scheduler.SchedulerTestUtils.createAgentInstance;
import static com.netflix.titus.master.scheduler.SchedulerTestUtils.createAgentInstanceGroup;
import static com.netflix.titus.master.scheduler.SchedulerTestUtils.createAssignableVirtualMachineMock;
import static com.netflix.titus.master.scheduler.SchedulerTestUtils.createStartLaunchedTask;
import static com.netflix.titus.master.scheduler.SchedulerTestUtils.createTaskRequest;
import static com.netflix.titus.master.scheduler.SchedulerTestUtils.createVirtualMachineCurrentStateMock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SchedulingMachinesFilterTest {

    private final SchedulerConfiguration schedulerConfiguration = mock(SchedulerConfiguration.class);
    private final AgentManagementService agentManagementService = mock(AgentManagementService.class);
    private final AgentStatusMonitor agentStatusMonitor = mock(AgentStatusMonitor.class);
    private final V3JobOperations jobOperations = mock(V3JobOperations.class);

    private final SchedulingMachinesFilter machinesFilter = new SchedulingMachinesFilter(schedulerConfiguration,
            agentManagementService, agentStatusMonitor, jobOperations);

    @Before
    public void setUp() throws Exception {
        when(schedulerConfiguration.getInstanceAttributeName()).thenReturn("id");
        when(schedulerConfiguration.isSchedulingMachinesFilterEnabled()).thenReturn(true);
        when(agentStatusMonitor.isHealthy(any())).thenReturn(true);
    }

    @Test
    public void instanceGroupNotFound() {
        AgentInstance instance = createAgentInstance(INSTANCE_ID, "");
        when(agentManagementService.findAgentInstance(INSTANCE_ID)).thenReturn(Optional.of(instance));
        VirtualMachineCurrentState currentState = createVirtualMachineCurrentStateMock(INSTANCE_ID);
        AssignableVirtualMachine machine = createAssignableVirtualMachineMock(currentState);
        List<AssignableVirtualMachine> filteredMachines = machinesFilter.filter(Collections.singletonList(machine));
        assertThat(filteredMachines).isEmpty();
    }

    @Test
    public void instanceGroupNotActive() {
        AgentInstance instance = createAgentInstance(INSTANCE_ID, UNKNOWN_INSTANCE_GROUP_ID);
        when(agentManagementService.findAgentInstance(INSTANCE_ID)).thenReturn(Optional.of(instance));
        AgentInstanceGroup agentInstanceGroup = createAgentInstanceGroup(UNKNOWN_INSTANCE_GROUP_ID,
                InstanceGroupLifecycleState.Inactive, Tier.Flex);
        when(agentManagementService.findInstanceGroup(INSTANCE_GROUP_ID)).thenReturn(Optional.of(agentInstanceGroup));
        VirtualMachineCurrentState currentState = createVirtualMachineCurrentStateMock(INSTANCE_ID);
        AssignableVirtualMachine machine = createAssignableVirtualMachineMock(currentState);
        List<AssignableVirtualMachine> filteredMachines = machinesFilter.filter(Collections.singletonList(machine));
        assertThat(filteredMachines).isEmpty();
    }

    @Test
    public void instanceGroupSystemNoPlacement() {
        AgentInstance instance = createAgentInstance(INSTANCE_ID, INSTANCE_GROUP_ID);
        when(agentManagementService.findAgentInstance(INSTANCE_ID)).thenReturn(Optional.of(instance));
        AgentInstanceGroup agentInstanceGroup = createAgentInstanceGroup(INSTANCE_GROUP_ID, InstanceGroupLifecycleState.Active,
                Tier.Flex, Collections.singletonMap(SchedulerAttributes.SYSTEM_NO_PLACEMENT, "true"));
        when(agentManagementService.findInstanceGroup(INSTANCE_GROUP_ID)).thenReturn(Optional.of(agentInstanceGroup));
        VirtualMachineCurrentState currentState = createVirtualMachineCurrentStateMock(INSTANCE_ID);
        AssignableVirtualMachine machine = createAssignableVirtualMachineMock(currentState);
        List<AssignableVirtualMachine> filteredMachines = machinesFilter.filter(Collections.singletonList(machine));
        assertThat(filteredMachines).isEmpty();
    }

    @Test
    public void instanceGroupNoPlacement() {
        AgentInstance instance = createAgentInstance(INSTANCE_ID, INSTANCE_GROUP_ID);
        when(agentManagementService.findAgentInstance(INSTANCE_ID)).thenReturn(Optional.of(instance));
        AgentInstanceGroup agentInstanceGroup = createAgentInstanceGroup(INSTANCE_GROUP_ID, InstanceGroupLifecycleState.Active,
                Tier.Flex, Collections.singletonMap(SchedulerAttributes.NO_PLACEMENT, "true"));
        when(agentManagementService.findInstanceGroup(INSTANCE_GROUP_ID)).thenReturn(Optional.of(agentInstanceGroup));
        VirtualMachineCurrentState currentState = createVirtualMachineCurrentStateMock(INSTANCE_ID);
        AssignableVirtualMachine machine = createAssignableVirtualMachineMock(currentState);
        List<AssignableVirtualMachine> filteredMachines = machinesFilter.filter(Collections.singletonList(machine));
        assertThat(filteredMachines).isEmpty();
    }

    @Test
    public void agentHasConcurrentLaunchLimitFromPreviousEvaluation() {
        Task startedTask = createStartLaunchedTask(TASK_ID);
        when(jobOperations.getTasks()).thenReturn(Collections.singletonList(startedTask));

        TaskRequest taskRequest = createTaskRequest(TASK_ID);

        VirtualMachineCurrentState currentState = createVirtualMachineCurrentStateMock(INSTANCE_ID,
                Collections.singletonList(taskRequest), Collections.emptyList());
        AssignableVirtualMachine machine = createAssignableVirtualMachineMock(currentState);
        List<AssignableVirtualMachine> filteredMachines = machinesFilter.filter(Collections.singletonList(machine));
        assertThat(filteredMachines).isEmpty();
    }
}