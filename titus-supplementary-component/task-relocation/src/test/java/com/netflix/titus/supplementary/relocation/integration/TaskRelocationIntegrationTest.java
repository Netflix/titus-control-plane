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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan.TaskRelocationReason;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.grpc.protogen.RelocationEvent;
import com.netflix.titus.grpc.protogen.RelocationEvent.EventCase;
import com.netflix.titus.grpc.protogen.RelocationTaskId;
import com.netflix.titus.grpc.protogen.TaskRelocationExecution;
import com.netflix.titus.grpc.protogen.TaskRelocationPlan;
import com.netflix.titus.grpc.protogen.TaskRelocationPlans;
import com.netflix.titus.grpc.protogen.TaskRelocationQuery;
import com.netflix.titus.grpc.protogen.TaskRelocationServiceGrpc;
import com.netflix.titus.grpc.protogen.TaskRelocationStatus;
import com.netflix.titus.supplementary.relocation.RelocationConnectorStubs;
import com.netflix.titus.supplementary.relocation.TestDataFactory;
import com.netflix.titus.testkit.grpc.TestStreamObserver;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import io.grpc.Status;
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

    private TaskRelocationServiceGrpc.TaskRelocationServiceStub client;

    @Before
    public void setUp() {
        this.sandbox = serverResource.getTaskRelocationSandbox();
        this.client = TaskRelocationServiceGrpc.newStub(sandbox.getGrpcChannel());
    }

    @Test(timeout = 60_000)
    public void testPlannedRelocation() throws Exception {
        Task task = createAndPlaceOneTaskJob(TestDataFactory.REMOVABLE_INSTANCE_GROUP);
        relocationConnectorStubs.setQuota(task.getJobId(), 1);

        // Get the plan
        TaskRelocationPlan plan = doTry(() -> findRelocationPlan(task.getId()));
        assertThat(plan.getTaskId()).isEqualTo(task.getId());
        assertThat(plan.getReasonCode()).isEqualTo(TaskRelocationReason.TaskMigration.name());
        assertThat(plan.getRelocationTime()).isLessThanOrEqualTo(clock.wallTime() + RELOCATION_TIME_MS);

        // Wait for the relocation
        TaskRelocationStatus status = doTry(() -> findRelocationStatus(task.getId()));
        assertThat(status.getState()).isEqualTo(TaskRelocationStatus.TaskRelocationState.Success);
        assertThat(status.getStatusCode()).isEqualTo(com.netflix.titus.api.relocation.model.TaskRelocationStatus.STATUS_CODE_TERMINATED);
        assertThat(status.getStatusMessage()).isNotEmpty();
    }

    @Test(timeout = 60_000)
    public void testEvents() throws InterruptedException {

        TestStreamObserver<RelocationEvent> events = new TestStreamObserver<>();
        client.observeRelocationEvents(TaskRelocationQuery.getDefaultInstance(), events);
        RelocationEvent firstEvent = events.takeNext(30, TimeUnit.SECONDS);
        assertThat(firstEvent).isNotNull();
        assertThat(firstEvent.getEventCase()).isEqualTo(EventCase.SNAPSHOTEND);

        Task task = createAndPlaceOneTaskJob(TestDataFactory.REMOVABLE_INSTANCE_GROUP);
        relocationConnectorStubs.setQuota(task.getJobId(), 1);

        RelocationEvent secondEvent = events.takeNext(30, TimeUnit.SECONDS);
        assertThat(secondEvent).isNotNull();
        assertThat(secondEvent.getEventCase()).isEqualTo(EventCase.TASKRELOCATIONPLANUPDATEEVENT);
        RelocationEvent thirdEvent = events.takeNext(30, TimeUnit.SECONDS);
        assertThat(thirdEvent).isNotNull();
        assertThat(thirdEvent.getEventCase()).isEqualTo(EventCase.TASKRELOCATIONPLANREMOVEEVENT);
    }

    private Task createAndPlaceOneTaskJob(String instanceGroup) {
        Job<BatchJobExt> job = TestDataFactory.newBatchJob("job1", 1, newSelfManagedDisruptionBudget(RELOCATION_TIME_MS));
        relocationConnectorStubs.addJob(job);

        Task task = jobOperations.getTasks().get(0);
        relocationConnectorStubs.place(instanceGroup, task);

        return task;
    }

    private Optional<TaskRelocationPlan> findRelocationPlan(String taskId) {
        TaskRelocationPlans plans;
        try {
            TestStreamObserver<TaskRelocationPlans> events = new TestStreamObserver<>();
            client.getCurrentTaskRelocationPlans(TaskRelocationQuery.getDefaultInstance(), events);
            plans = events.getLast();
        } catch (Exception e) {
            if (e.getMessage().contains("Relocation workflow not ready yet")) {
                return Optional.empty();
            }
            throw new RuntimeException(e);
        }

        Optional<TaskRelocationPlan> taskPlan = plans.getPlansList().stream().filter(p -> p.getTaskId().equals(taskId)).findFirst();
        if (taskPlan.isPresent()) {
            return taskPlan;
        }

        // Check if already processed
        try {
            TestStreamObserver<TaskRelocationExecution> events = new TestStreamObserver<>();
            client.getTaskRelocationResult(RelocationTaskId.newBuilder().setId(taskId).build(), events);
            return Optional.of(events.getLast().getTaskRelocationPlan());
        } catch (Exception e) {
            Status status = Status.fromThrowable(e);
            if (status.getCode() == Status.Code.NOT_FOUND) {
                return Optional.empty();
            }
            throw new RuntimeException(e);
        }
    }

    private Optional<TaskRelocationStatus> findRelocationStatus(String taskId) {
        try {
            TestStreamObserver<TaskRelocationExecution> events = new TestStreamObserver<>();
            client.getTaskRelocationResult(RelocationTaskId.newBuilder().setId(taskId).build(), events);
            return Optional.of(last(events.getLast().getRelocationAttemptsList()));
        } catch (Exception e) {
            Status status = Status.fromThrowable(e);
            if (status.getCode() == Status.Code.NOT_FOUND) {
                return Optional.empty();
            }
            throw new RuntimeException(e);
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
