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

import java.util.Collections;
import java.util.Optional;

import com.netflix.fenzo.ConstraintEvaluator.Result;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;
import com.netflix.titus.testkit.model.agent.AgentGenerator;
import org.junit.Before;
import org.junit.Test;

import static com.netflix.titus.master.scheduler.SchedulerTestUtils.createTaskRequest;
import static com.netflix.titus.master.scheduler.SchedulerTestUtils.createTaskTrackerState;
import static com.netflix.titus.master.scheduler.SchedulerTestUtils.createVirtualMachineCurrentStateMock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MachineTypeConstraintTest {
    private static final String MACHINE_ID = "1234";
    private static final String MACHINE_TYPE = "machineType1";
    private static final String MACHINE_TYPE_ATTRIBUTE_NAME = "machineType";

    private final SchedulerConfiguration schedulerConfiguration = mock(SchedulerConfiguration.class);
    private final AgentManagementService agentManagementService = mock(AgentManagementService.class);

    private final MachineTypeConstraint constraint = new MachineTypeConstraint(schedulerConfiguration, agentManagementService, MACHINE_TYPE);

    @Before
    public void setUp() throws Exception {
        when(schedulerConfiguration.getInstanceAttributeName()).thenReturn("id");
        when(schedulerConfiguration.getMachineTypeAttributeName()).thenReturn(MACHINE_TYPE_ATTRIBUTE_NAME);
    }

    @Test
    public void machineDoesNotExist() {
        when(agentManagementService.findAgentInstance(MACHINE_ID)).thenReturn(Optional.empty());
        Result result = constraint.evaluate(createTaskRequest(), createVirtualMachineCurrentStateMock(MACHINE_ID), createTaskTrackerState());
        assertThat(result.isSuccessful()).isFalse();
        assertThat(result.getFailureReason()).isEqualToIgnoringCase("The machine does not exist");
    }

    @Test
    public void machineTypeDoesNotMatch() {
        AgentInstance instance = AgentGenerator.agentInstances().getValue().toBuilder()
                .withId(MACHINE_ID)
                .withAttributes(Collections.singletonMap(MACHINE_TYPE_ATTRIBUTE_NAME, "noMatch"))
                .build();
        when(agentManagementService.findAgentInstance(MACHINE_ID)).thenReturn(Optional.of(instance));
        Result result = constraint.evaluate(createTaskRequest(), createVirtualMachineCurrentStateMock(MACHINE_ID), createTaskTrackerState());
        assertThat(result.isSuccessful()).isFalse();
        assertThat(result.getFailureReason()).isEqualToIgnoringCase("The machine type does not match the specified machine type");
    }

    @Test
    public void machineTypeDoesMatch() {
        AgentInstance instance = AgentGenerator.agentInstances().getValue().toBuilder()
                .withId(MACHINE_ID)
                .withAttributes(Collections.singletonMap(MACHINE_TYPE_ATTRIBUTE_NAME, MACHINE_TYPE))
                .build();
        when(agentManagementService.findAgentInstance(MACHINE_ID)).thenReturn(Optional.of(instance));
        Result result = constraint.evaluate(createTaskRequest(), createVirtualMachineCurrentStateMock(MACHINE_ID), createTaskTrackerState());
        assertThat(result.isSuccessful()).isTrue();
    }
}