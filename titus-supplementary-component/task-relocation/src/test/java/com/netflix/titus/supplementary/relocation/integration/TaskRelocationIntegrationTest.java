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

package com.netflix.titus.supplementary.relocation.integration;

import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.grpc.protogen.RelocationTaskId;
import com.netflix.titus.grpc.protogen.TaskRelocationExecution;
import com.netflix.titus.grpc.protogen.TaskRelocationPlan;
import com.netflix.titus.grpc.protogen.TaskRelocationPlans;
import com.netflix.titus.grpc.protogen.TaskRelocationQuery;
import com.netflix.titus.grpc.protogen.TaskRelocationServiceGrpc;
import com.netflix.titus.grpc.protogen.TaskRelocationServiceGrpc.TaskRelocationServiceBlockingStub;
import com.netflix.titus.grpc.protogen.TaskRelocationStatus;
import com.netflix.titus.supplementary.relocation.RelocationConnectorStubs;
import com.netflix.titus.supplementary.relocation.TestDataFactory;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan.TaskRelocationReason;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static com.netflix.titus.common.util.CollectionsExt.last;
import static com.netflix.titus.supplementary.relocation.TestDataFactory.newSelfManagedDisruptionBudget;
import static org.assertj.core.api.Assertions.assertThat;

@Category(value = IntegrationTest.class)
public class TaskRelocationIntegrationTest {

    private static final int RELOCATION_TIME_MS = 100;

    private final TitusRuntime titusRuntime = TitusRuntimes.internal();
    private final Clock clock = titusRuntime.getClock();

    private final RelocationConnectorStubs relocationConnectorStubs = TestDataFactory.activeRemovableSetup(titusRuntime);
    private final ReadOnlyJobOperations jobOperations = relocationConnectorStubs.getJobOperations();

    @Rule
    public final TaskRelocationResourceSandbox serverResource = new TaskRelocationResourceSandbox(relocationConnectorStubs);

    private TaskRelocationSandbox sandbox;

    private TaskRelocationServiceBlockingStub client;

    @Before
    public void setUp() {
        this.sandbox = serverResource.getTaskRelocationSandbox();
        this.client = TaskRelocationServiceGrpc.newBlockingStub(sandbox.getGrpcChannel());
    }

    @Test(timeout = 60_000)
    public void testPlannedRelocation() throws Exception {
        Task task = placeTaskOnRemovableAgent();
        relocationConnectorStubs.setQuota(task.getJobId(), 1);

        // Get the plan
        TaskRelocationPlan plan = doTry(() -> findRelocationPlan(task.getId()));
        assertThat(plan.getTaskId()).isEqualTo(task.getId());
        assertThat(plan.getReasonCode()).isEqualTo(TaskRelocationReason.TaskMigration.name());
        assertThat(plan.getRelocationTime()).isLessThanOrEqualTo(clock.wallTime() + RELOCATION_TIME_MS);

        // Wait for the relocation
        TaskRelocationStatus status = doTry(() -> findRelocationStatus(task.getId()));
        assertThat(status.getState()).isEqualTo(TaskRelocationStatus.TaskRelocationState.Success);
        assertThat(status.getReasonCode()).isEqualTo(com.netflix.titus.api.relocation.model.TaskRelocationStatus.REASON_CODE_TERMINATED);
        assertThat(status.getReasonMessage()).isNotEmpty();
    }

    private Task placeTaskOnRemovableAgent() {
        Job<BatchJobExt> job = TestDataFactory.newBatchJob("job1", 1, newSelfManagedDisruptionBudget(RELOCATION_TIME_MS));
        relocationConnectorStubs.addJob(job);

        Task task = jobOperations.getTasks().get(0);
        relocationConnectorStubs.place(TestDataFactory.REMOVABLE_INSTANCE_GROUP, task);

        return task;
    }

    private Optional<TaskRelocationPlan> findRelocationPlan(String taskId) {
        TaskRelocationPlans plans = client.getCurrentTaskRelocationPlans(TaskRelocationQuery.getDefaultInstance());

        Optional<TaskRelocationPlan> taskPlan = plans.getPlansList().stream().filter(p -> p.getTaskId().equals(taskId)).findFirst();
        if (taskPlan.isPresent()) {
            return taskPlan;
        }

        // Check if already processed
        try {
            TaskRelocationExecution status = client.getTaskRelocationResult(RelocationTaskId.newBuilder().setId(taskId).build());
            return Optional.of(status.getTaskRelocationPlan());
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                return Optional.empty();
            }
            throw e;
        }
    }

    private Optional<TaskRelocationStatus> findRelocationStatus(String taskId) {
        try {
            TaskRelocationExecution status = client.getTaskRelocationResult(RelocationTaskId.newBuilder().setId(taskId).build());
            return Optional.of(last(status.getRelocationAttemptsList()));
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                return Optional.empty();
            }
            throw e;
        }
    }

    private <T> T doTry(Supplier<Optional<T>> valueSupplier) throws Exception {
        long deadline = System.currentTimeMillis() + 5_000;
        while (deadline > System.currentTimeMillis()) {
            Optional<T> result = valueSupplier.get();
            if (result.isPresent()) {
                return result.get();
            }
            Thread.sleep(10);
        }
        throw new TimeoutException();
    }
}
