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
import java.util.Optional;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.supplementary.relocation.AbstractTaskRelocationTest;
import com.netflix.titus.supplementary.relocation.TestDataFactory;
import com.netflix.titus.supplementary.relocation.model.TaskRelocationPlan;
import com.netflix.titus.supplementary.relocation.store.TaskRelocationStore;
import org.junit.Test;
import reactor.core.publisher.Mono;

import static com.netflix.titus.supplementary.relocation.TaskRelocationPlanGenerator.oneMigrationPlan;
import static com.netflix.titus.supplementary.relocation.TestDataFactory.newSelfManagedDisruptionBudget;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MustBeRelocatedTaskStoreUpdateStepTest extends AbstractTaskRelocationTest {

    private final TaskRelocationStore store = mock(TaskRelocationStore.class);

    private final RelocationTransactionLogger transactionLog = new RelocationTransactionLogger(jobOperations);

    public MustBeRelocatedTaskStoreUpdateStepTest() {
        super(TestDataFactory.activeRemovableSetup());
    }

    @Test
    public void testCreateOrUpdate() {
        when(store.getAllTaskRelocationPlans()).thenReturn(Mono.just(Collections.emptyMap()));
        MustBeRelocatedTaskStoreUpdateStep step = new MustBeRelocatedTaskStoreUpdateStep(store, transactionLog, titusRuntime);

        Job<BatchJobExt> job = TestDataFactory.newBatchJob("job1", 1, newSelfManagedDisruptionBudget(1_000));
        relocationConnectorStubs.addJob(job);
        relocationConnectorStubs.place(TestDataFactory.REMOVABLE_INSTANCE_GROUP, jobOperations.getTasks().get(0));

        when(store.createOrUpdateTaskRelocationPlans(anyList())).thenReturn(Mono.just(Collections.singletonMap("task1", Optional.empty())));

        TaskRelocationPlan taskRelocationPlan = oneMigrationPlan();
        step.persistChangesInStore(
                Collections.singletonMap(taskRelocationPlan.getTaskId(), taskRelocationPlan)
        );

        verify(store, times(1)).createOrUpdateTaskRelocationPlans(Collections.singletonList(taskRelocationPlan));

        // No try again with the same data, and make sure no store update is executed
        step.persistChangesInStore(
                Collections.singletonMap(taskRelocationPlan.getTaskId(), taskRelocationPlan)
        );
        verify(store, times(1)).createOrUpdateTaskRelocationPlans(Collections.singletonList(taskRelocationPlan));
    }

    @Test
    public void testRemove() {
        TaskRelocationPlan taskRelocationPlan = oneMigrationPlan();
        when(store.getAllTaskRelocationPlans()).thenReturn(Mono.just(Collections.singletonMap(taskRelocationPlan.getTaskId(), taskRelocationPlan)));
        MustBeRelocatedTaskStoreUpdateStep step = new MustBeRelocatedTaskStoreUpdateStep(store, transactionLog, titusRuntime);

        when(store.removeTaskRelocationPlans(Collections.singleton(taskRelocationPlan.getTaskId()))).thenReturn(Mono.just(Collections.singletonMap("task1", Optional.empty())));
        step.persistChangesInStore(Collections.emptyMap());

        verify(store, times(1)).removeTaskRelocationPlans(Collections.singleton(taskRelocationPlan.getTaskId()));

        // No try again with the same data, and make sure no store update is executed
        step.persistChangesInStore(Collections.emptyMap());
        verify(store, times(1)).removeTaskRelocationPlans(Collections.singleton(taskRelocationPlan.getTaskId()));
    }
}