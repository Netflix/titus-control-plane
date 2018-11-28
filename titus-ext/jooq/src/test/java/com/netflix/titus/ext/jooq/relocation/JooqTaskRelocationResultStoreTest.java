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

package com.netflix.titus.ext.jooq.relocation;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.netflix.titus.api.relocation.model.TaskRelocationPlan;
import com.netflix.titus.api.relocation.model.TaskRelocationStatus;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JooqTaskRelocationResultStoreTest {

    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    @Rule
    public final JooqResource jooqResource = new JooqResource();

    private JooqTaskRelocationResultStore store;

    @Before
    public void setUp() {
        this.store = newStore();
    }

    private JooqTaskRelocationResultStore newStore() {
        return new JooqTaskRelocationResultStore(jooqResource.getDslContext(), titusRuntime);
    }

    @Test
    public void testRelocationStatusStoreCrud() {
        TaskRelocationStatus status = newRelocationStatus();

        // Create
        Map<String, Optional<Throwable>> result = store.createTaskRelocationStatuses(Collections.singletonList(status)).block();
        assertThat(result).hasSize(1);
        assertThat(result.get(status.getTaskId())).isEmpty();

        // Reboot (to force reload from the database).
        this.store = newStore();

        // Read
        List<TaskRelocationStatus> statusList = store.getTaskRelocationStatusList(status.getTaskId()).block();
        assertThat(statusList).hasSize(1);
        assertThat(statusList.get(0)).isEqualTo(status);

        // Update
        TaskRelocationStatus updatedStatus = status.toBuilder().withReasonMessage("Updated...").build();
        Map<String, Optional<Throwable>> updatedResult = store.createTaskRelocationStatuses(Collections.singletonList(updatedStatus)).block();
        assertThat(updatedResult).hasSize(1);
        assertThat(store.getTaskRelocationStatusList(status.getTaskId()).block().get(0)).isEqualTo(updatedStatus);

        // Reboot (to force reload from the database).
        this.store = newStore();

        // Read
        assertThat(store.getTaskRelocationStatusList(status.getTaskId()).block().get(0)).isEqualTo(updatedStatus);
    }

    private TaskRelocationStatus newRelocationStatus() {
        return TaskRelocationStatus.newBuilder()
                .withTaskId("task1")
                .withState(TaskRelocationStatus.TaskRelocationState.Success)
                .withReasonCode("reason123")
                .withReasonMessage("reasonMessage123")
                .withTaskRelocationPlan(TaskRelocationPlan.newBuilder()
                        .withTaskId("task1")
                        .withReason(TaskRelocationPlan.TaskRelocationReason.TaskMigration)
                        .withReasonMessage("Test...")
                        .withRelocationTime(123)
                        .build()
                )
                .build();
    }
}