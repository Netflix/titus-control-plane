/*
 * Copyright 2020 Netflix, Inc.
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

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;
import org.junit.Test;

import static com.netflix.titus.master.scheduler.SchedulerTestUtils.INSTANCE_ID;
import static com.netflix.titus.master.scheduler.SchedulerTestUtils.TASK_ID;
import static com.netflix.titus.master.scheduler.SchedulerTestUtils.createMockAssignmentResultList;
import static com.netflix.titus.master.scheduler.SchedulerTestUtils.createTaskRequest;
import static com.netflix.titus.master.scheduler.SchedulerTestUtils.createVirtualMachineCurrentStateMock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AgentContainerLimitSystemConstraintTest {

    private final SchedulerConfiguration schedulerConfiguration = mock(SchedulerConfiguration.class);

    private final AgentContainerLimitSystemConstraint constraint = new AgentContainerLimitSystemConstraint(schedulerConfiguration);
    private final TaskTrackerState taskTrackerState = mock(TaskTrackerState.class);

    @Test
    public void testAtLeastOneContainerCanBeScheduled() {
        when(schedulerConfiguration.getMaxLaunchingTasksPerMachine()).thenReturn(0);
        ConstraintEvaluator.Result result = constraint.evaluate(
                createTaskRequest(TASK_ID),
                createVirtualMachineCurrentStateMock(INSTANCE_ID, Collections.emptyList(), Collections.emptyList()),
                taskTrackerState);
        assertThat(result.isSuccessful()).isTrue();
    }

    @Test
    public void testContainerLimit() {
        when(schedulerConfiguration.getMaxLaunchingTasksPerMachine()).thenReturn(2);
        ConstraintEvaluator.Result result = constraint.evaluate(
                createTaskRequest(TASK_ID),
                createVirtualMachineCurrentStateMock(INSTANCE_ID, Collections.emptyList(), createMockAssignmentResultList(2)),
                taskTrackerState);
        assertThat(result.isSuccessful()).isFalse();
    }
}