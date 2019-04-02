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

import com.netflix.titus.supplementary.relocation.AbstractTaskRelocationTest;
import com.netflix.titus.testkit.model.relocation.TaskRelocationPlanGenerator;
import com.netflix.titus.supplementary.relocation.TestDataFactory;
import com.netflix.titus.api.relocation.model.TaskRelocationStatus;
import com.netflix.titus.supplementary.relocation.store.TaskRelocationResultStore;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Mono;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TaskEvictionResultStoreStepTest extends AbstractTaskRelocationTest {

    private final RelocationTransactionLogger transactionLog = new RelocationTransactionLogger(jobOperations);

    private TaskRelocationResultStore store = mock(TaskRelocationResultStore.class);

    private TaskEvictionResultStoreStep step;

    public TaskEvictionResultStoreStepTest() {
        super(TestDataFactory.activeRemovableSetup());
    }

    @Before
    public void setUp() {
        this.step = new TaskEvictionResultStoreStep(store, transactionLog, titusRuntime);
    }

    @Test
    public void testSuccessfulStore() {
        TaskRelocationStatus relocationStatus = TaskRelocationPlanGenerator.oneSuccessfulRelocation();

        when(store.createTaskRelocationStatuses(anyList())).thenReturn(
                Mono.just(Collections.singletonMap(relocationStatus.getTaskId(), Optional.empty()))
        );

        step.storeTaskEvictionResults(Collections.singletonMap(relocationStatus.getTaskId(), relocationStatus));

        verify(store, times(1)).createTaskRelocationStatuses(anyList());
    }

    @Test
    public void testStoreFailure() {
        when(store.createTaskRelocationStatuses(anyList())).thenReturn(Mono.error(new RuntimeException("Simulated store error")));

        TaskRelocationStatus relocationStatus = TaskRelocationPlanGenerator.oneSuccessfulRelocation();
        step.storeTaskEvictionResults(Collections.singletonMap(relocationStatus.getTaskId(), relocationStatus));

        verify(store, times(1)).createTaskRelocationStatuses(anyList());
    }
}