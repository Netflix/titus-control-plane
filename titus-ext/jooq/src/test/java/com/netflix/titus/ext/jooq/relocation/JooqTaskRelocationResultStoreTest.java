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
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.mock.env.MockEnvironment;

import static org.assertj.core.api.Assertions.assertThat;

public class JooqTaskRelocationResultStoreTest {

    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    @Rule
    public final JooqResource jooqResource = new JooqResource();

    private final JooqRelocationConfiguration configuration = Archaius2Ext.newConfiguration(JooqRelocationConfiguration.class, new MockEnvironment());

    private JooqTaskRelocationResultStore store;

    @Before
    public void setUp() {
        this.store = newStore();
    }

    @Test
    public void testRelocationStatusStoreCrud() {
        List<TaskRelocationStatus> statusList = newRelocationStatuses("task", 1, System.currentTimeMillis());
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
        List<TaskRelocationStatus> statusList = newRelocationStatuses("task", 10_000, System.currentTimeMillis());

        // Create
        Map<String, Optional<Throwable>> result = store.createTaskRelocationStatuses(statusList).block();
        assertThat(result).hasSize(statusList.size());
        long failures = result.values().stream().filter(Optional::isPresent).count();
        assertThat(failures).isZero();
    }

    @Test
    public void testGC() {
        long now = System.currentTimeMillis();
        List<TaskRelocationStatus> statusList = CollectionsExt.merge(
                newRelocationStatuses("old", 1, now - 3_600_000),
                newRelocationStatuses("new", 1, now - 60_000)
        );
        store.createTaskRelocationStatuses(statusList).block();

        JooqTaskRelocationGC gc = new JooqTaskRelocationGC(configuration, jooqResource.getDslContext(), store, titusRuntime);
        int removed = gc.removeExpiredData(now - 3_000_000);
        assertThat(removed).isEqualTo(1);

        List<TaskRelocationStatus> oldTaskStatus = store.getTaskRelocationStatusList("old0").block();
        assertThat(oldTaskStatus).isEmpty();

        List<TaskRelocationStatus> newTaskStatus = store.getTaskRelocationStatusList("new0").block();
        assertThat(newTaskStatus).hasSize(1);
    }

    private JooqTaskRelocationResultStore newStore() {
        return new JooqTaskRelocationResultStore(jooqResource.getDslContext(), titusRuntime);
    }

    private List<TaskRelocationStatus> newRelocationStatuses(String taskPrefix, int count, long executionTime) {
        List<TaskRelocationStatus> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            result.add(TaskRelocationStatus.newBuilder()
                    .withTaskId(taskPrefix + i)
                    .withState(TaskRelocationStatus.TaskRelocationState.Success)
                    .withStatusCode("status123")
                    .withStatusMessage("statusMessage123")
                    .withTimestamp(executionTime)
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