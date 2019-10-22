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

package com.netflix.titus.master.scheduler.constraint;

import java.util.Collections;
import java.util.List;

import com.netflix.fenzo.ConstraintEvaluator.Result;
import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;
import org.junit.Before;
import org.junit.Test;

import static com.netflix.titus.master.scheduler.SchedulerTestUtils.INSTANCE_ID;
import static com.netflix.titus.master.scheduler.SchedulerTestUtils.TASK_ID;
import static com.netflix.titus.master.scheduler.SchedulerTestUtils.createMockAssignmentResultList;
import static com.netflix.titus.master.scheduler.SchedulerTestUtils.createStartLaunchedTask;
import static com.netflix.titus.master.scheduler.SchedulerTestUtils.createTaskRequest;
import static com.netflix.titus.master.scheduler.SchedulerTestUtils.createVirtualMachineCurrentStateMock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AgentLaunchGuardConstraintTest {
    private final SchedulerConfiguration schedulerConfiguration = mock(SchedulerConfiguration.class);
    private final V3JobOperations jobOperations = mock(V3JobOperations.class);

    private final AgentLaunchGuardConstraint agentLaunchGuardConstraint = new AgentLaunchGuardConstraint(schedulerConfiguration, jobOperations);
    private final TaskTrackerState taskTrackerState = mock(TaskTrackerState.class);

    @Before
    public void setUp() throws Exception {
        when(schedulerConfiguration.isGlobalTaskLaunchingConstraintEvaluatorEnabled()).thenReturn(true);
        when(schedulerConfiguration.getMaxLaunchingTasksPerMachine()).thenReturn(1);
        when(schedulerConfiguration.getInstanceAttributeName()).thenReturn("id");
    }

    @Test
    public void zeroConcurrentLaunchLimitPreventsScheduling() {
        when(schedulerConfiguration.getMaxLaunchingTasksPerMachine()).thenReturn(0);
        Result result = agentLaunchGuardConstraint.evaluate(
                createTaskRequest(TASK_ID),
                createVirtualMachineCurrentStateMock(INSTANCE_ID, Collections.emptyList(), Collections.emptyList()),
                taskTrackerState);
        assertThat(result.isSuccessful()).isFalse();
    }

    @Test
    public void agentHasLessThanConcurrentLaunchesLimit() {
        Result result = agentLaunchGuardConstraint.evaluate(
                createTaskRequest(TASK_ID),
                createVirtualMachineCurrentStateMock(INSTANCE_ID, Collections.emptyList(), Collections.emptyList()),
                taskTrackerState);
        assertThat(result.isSuccessful()).isTrue();
    }

    @Test
    public void agentHasConcurrentLaunchLimitFromPreviousEvaluation() {
        Task startedTask = createStartLaunchedTask(TASK_ID);
        when(jobOperations.getTasks()).thenReturn(Collections.singletonList(startedTask));

        TaskRequest taskRequest = createTaskRequest(TASK_ID);

        agentLaunchGuardConstraint.prepare();
        Result result = agentLaunchGuardConstraint.evaluate(
                taskRequest,
                createVirtualMachineCurrentStateMock(INSTANCE_ID, Collections.singletonList(taskRequest), Collections.emptyList()),
                taskTrackerState);
        assertThat(result.isSuccessful()).isFalse();
    }

    @Test
    public void agentHasConcurrentLaunchLimitFromCurrentEvaluation() {
        Task startedTask = createStartLaunchedTask(TASK_ID);
        when(jobOperations.getTasks()).thenReturn(Collections.singletonList(startedTask));

        TaskRequest taskRequest = createTaskRequest(TASK_ID);
        List<TaskAssignmentResult> assignmentResultList = createMockAssignmentResultList(1);
        agentLaunchGuardConstraint.prepare();
        Result result = agentLaunchGuardConstraint.evaluate(
                taskRequest,
                createVirtualMachineCurrentStateMock(INSTANCE_ID, Collections.singletonList(taskRequest), assignmentResultList),
                taskTrackerState);
        assertThat(result.isSuccessful()).isFalse();
    }
}