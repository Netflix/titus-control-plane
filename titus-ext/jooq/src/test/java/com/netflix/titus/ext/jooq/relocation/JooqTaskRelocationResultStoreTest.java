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

import java.util.ArrayList;
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

    @Test
    public void testRelocationStatusStoreCrud() {
        List<TaskRelocationStatus> statusList = newRelocationStatuses(1);
        TaskRelocationStatus status = statusList.get(0);

        // Create
        Map<String, Optional<Throwable>> result = store.createTaskRelocationStatuses(statusList).block();
        assertThat(result).hasSize(1);
        assertThat(result.get(status.getTaskId())).isEmpty();

        // Reboot (to force reload from the database).
        this.store = newStore();

        // Read
        List<TaskRelocationStatus> statusListRead = store.getTaskRelocationStatusList(status.getTaskId()).block();
        assertThat(statusListRead).hasSize(1);
        assertThat(statusListRead.get(0)).isEqualTo(status);

        // Update
        TaskRelocationStatus updatedStatus = status.toBuilder().withStatusMessage("Updated...").build();
        Map<String, Optional<Throwable>> updatedResult = store.createTaskRelocationStatuses(Collections.singletonList(updatedStatus)).block();
        assertThat(updatedResult).hasSize(1);
        assertThat(store.getTaskRelocationStatusList(status.getTaskId()).block().get(0)).isEqualTo(updatedStatus);

        // Reboot (to force reload from the database).
        this.store = newStore();

        // Read
        assertThat(store.getTaskRelocationStatusList(status.getTaskId()).block().get(0)).isEqualTo(updatedStatus);
    }

    @Test
    public void testStoringLargeAmountOfStatuses() {
        List<TaskRelocationStatus> statusList = newRelocationStatuses(10_000);

        // Create
        Map<String, Optional<Throwable>> result = store.createTaskRelocationStatuses(statusList).block();
        assertThat(result).hasSize(statusList.size());
        long failures = result.values().stream().filter(Optional::isPresent).count();
        assertThat(failures).isZero();
    }

    private JooqTaskRelocationResultStore newStore() {
        return new JooqTaskRelocationResultStore(jooqResource.getDslContext(), titusRuntime);
    }

    private List<TaskRelocationStatus> newRelocationStatuses(int count) {
        List<TaskRelocationStatus> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            result.add(TaskRelocationStatus.newBuilder()
                    .withTaskId("task" + i)
                    .withState(TaskRelocationStatus.TaskRelocationState.Success)
                    .withStatusCode("status123")
                    .withStatusMessage("statusMessage123")
                    .withTaskRelocationPlan(TaskRelocationPlan.newBuilder()
                            .withTaskId("task" + i)
                            .withReason(TaskRelocationPlan.TaskRelocationReason.TaskMigration)
                            .withReasonMessage("Test...")
                            .withRelocationTime(123)
                            .build()
                    )
                    .build()
            );
        }
        return result;
    }
}