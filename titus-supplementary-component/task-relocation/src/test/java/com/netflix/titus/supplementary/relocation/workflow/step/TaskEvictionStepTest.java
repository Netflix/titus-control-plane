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

import java.util.Collections;
import java.util.Map;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.supplementary.relocation.AbstractTaskRelocationTest;
import com.netflix.titus.supplementary.relocation.TestDataFactory;
import com.netflix.titus.supplementary.relocation.model.TaskRelocationPlan;
import com.netflix.titus.supplementary.relocation.model.TaskRelocationStatus;
import org.junit.Before;
import org.junit.Test;
import reactor.core.scheduler.Schedulers;

import static com.netflix.titus.supplementary.relocation.TaskRelocationPlanGenerator.oneMigrationPlan;
import static com.netflix.titus.supplementary.relocation.TestDataFactory.newSelfManagedDisruptionBudget;
import static org.assertj.core.api.Assertions.assertThat;

public class TaskEvictionStepTest extends AbstractTaskRelocationTest {

    private final RelocationTransactionLogger transactionLog = new RelocationTransactionLogger(jobOperations);

    private TaskEvictionStep step;

    public TaskEvictionStepTest() {
        super(TestDataFactory.activeRemovableSetup());
    }

    @Before
    public void setUp() {
        this.step = new TaskEvictionStep(evictionServiceClient, titusRuntime, transactionLog, Schedulers.parallel());
    }

    @Test
    public void testSuccessfulEviction() {
        Job<BatchJobExt> job = TestDataFactory.newBatchJob("job1", 1, newSelfManagedDisruptionBudget(1_000));
        relocationConnectorStubs.addJob(job);
        relocationConnectorStubs.setQuota("job1", 1);

        Task task = jobOperations.getTasks().get(0);
        relocationConnectorStubs.place(TestDataFactory.REMOVABLE_INSTANCE_GROUP, task);

        TaskRelocationPlan taskRelocationPlan = oneMigrationPlan().toBuilder().withTaskId(task.getId()).build();

        Map<String, TaskRelocationStatus> result = step.evict(Collections.singletonMap(task.getId(), taskRelocationPlan));
        assertThat(result).hasSize(1);

        TaskRelocationStatus relocationStatus = result.get(task.getId());
        assertThat(relocationStatus.getTaskId()).isEqualTo(task.getId());
        assertThat(relocationStatus.getReasonCode()).isEqualTo(TaskRelocationStatus.REASON_CODE_TERMINATED);
        assertThat(relocationStatus.getTaskRelocationPlan()).isEqualTo(taskRelocationPlan);
    }

    @Test
    public void testFailedEviction() {
        TaskRelocationPlan taskRelocationPlan = oneMigrationPlan().toBuilder().withTaskId("nonExistingTaskId").build();

        Map<String, TaskRelocationStatus> result = step.evict(Collections.singletonMap("nonExistingTaskId", taskRelocationPlan));
        assertThat(result).hasSize(1);

        TaskRelocationStatus relocationStatus = result.get("nonExistingTaskId");
        assertThat(relocationStatus.getTaskId()).isEqualTo("nonExistingTaskId");
        assertThat(relocationStatus.getReasonCode()).isEqualTo(TaskRelocationStatus.REASON_EVICTION_ERROR);
        assertThat(relocationStatus.getTaskRelocationPlan()).isEqualTo(taskRelocationPlan);
    }
}