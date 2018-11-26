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

package com.netflix.titus.supplementary.relocation.workflow.step;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.common.util.time.TestClock;
import com.netflix.titus.supplementary.relocation.AbstractTaskRelocationTest;
import com.netflix.titus.supplementary.relocation.TestDataFactory;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan;
import org.junit.Test;

import static com.netflix.titus.supplementary.relocation.TestDataFactory.newSelfManagedDisruptionBudget;
import static org.assertj.core.api.Assertions.assertThat;

public class MustBeRelocatedTaskCollectorStepTest extends AbstractTaskRelocationTest {

    private final MustBeRelocatedTaskCollectorStep step;

    public MustBeRelocatedTaskCollectorStepTest() {
        super(TestDataFactory.activeRemovableSetup());
        this.step = new MustBeRelocatedTaskCollectorStep(agentOperations, jobOperations, titusRuntime);
    }

    @Test
    public void testMigrationOfTasksWithPolicy() {
        Job<BatchJobExt> job = TestDataFactory.newBatchJob("job1", 1, newSelfManagedDisruptionBudget(1_000));
        relocationConnectorStubs.addJob(job);
        relocationConnectorStubs.place(TestDataFactory.REMOVABLE_INSTANCE_GROUP, jobOperations.getTasks().get(0));

        Map<String, TaskRelocationPlan> result = step.collectTasksThatMustBeRelocated();
        assertThat(result).hasSize(1);
    }

    @Test
    public void testTaskWithNoDisruptionBudgetIsNotMigrated() {
        Job<BatchJobExt> job = TestDataFactory.newBatchJob("job1", 1, JobFunctions.getNoDisruptionBudgetMarker());
        relocationConnectorStubs.addJob(job);
        relocationConnectorStubs.place(TestDataFactory.REMOVABLE_INSTANCE_GROUP, jobOperations.getTasks().get(0));

        Map<String, TaskRelocationPlan> result = step.collectTasksThatMustBeRelocated();
        assertThat(result).isEmpty();
    }

    @Test
    public void testOriginalPlanIsReturnedOnEachInvocation() {
        Job<BatchJobExt> job = TestDataFactory.newBatchJob("job1", 1, newSelfManagedDisruptionBudget(1_000));
        relocationConnectorStubs.addJob(job);
        Task task = jobOperations.getTasks().get(0);
        relocationConnectorStubs.place(TestDataFactory.REMOVABLE_INSTANCE_GROUP, task);

        Map<String, TaskRelocationPlan> firstResult = step.collectTasksThatMustBeRelocated();
        assertThat(firstResult).hasSize(1);
        TaskRelocationPlan first = firstResult.get(task.getId());

        ((TestClock)titusRuntime.getClock()).advanceTime(1, TimeUnit.SECONDS);

        Map<String, TaskRelocationPlan> secondResult = step.collectTasksThatMustBeRelocated();
        assertThat(secondResult).hasSize(1);
        TaskRelocationPlan second = secondResult.get(task.getId());

        assertThat(first).isEqualTo(second);
    }
}