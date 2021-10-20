/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.runtime.connector.jobmanager.snapshot;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.tuple.Pair;
import org.junit.Test;

import static com.netflix.titus.runtime.connector.jobmanager.snapshot.JobSnapshotTestUtil.newJobWithTasks;
import static com.netflix.titus.runtime.connector.jobmanager.snapshot.JobSnapshotTestUtil.newSnapshot;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Negative test scenarios only. Positive tests are in {@link JobSnapshotTest}.
 */
public class PCollectionJobSnapshotTest {

    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    private final List<String> inconsistenciesReports = new ArrayList<>();

    @Test
    public void testAutoFixMode() {
        Pair<Job<?>, Map<String, Task>> pair1 = newJobWithTasks(1, 2);
        List<Task> tasks1 = new ArrayList<>(pair1.getRight().values());

        JobSnapshot initial = newSnapshot(newFactory(true), pair1);
        JobSnapshot updated = initial.updateTask(tasks1.get(0), true).orElse(null);
        assertThat(updated).isNotNull();
        assertThat(inconsistenciesReports).hasSize(1);
    }

    @Test
    public void testNoAutoFixMode() {
        Pair<Job<?>, Map<String, Task>> pair1 = newJobWithTasks(1, 2);
        List<Task> tasks1 = new ArrayList<>(pair1.getRight().values());

        JobSnapshot initial = newSnapshot(newFactory(false), pair1);
        try {
            initial.updateTask(tasks1.get(0), true);
            fail("error expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IllegalStateException.class);
        }
        assertThat(inconsistenciesReports).hasSize(1);
    }

    private JobSnapshotFactory newFactory(boolean autoFix) {
        return JobSnapshotFactories.newDefault(autoFix, inconsistenciesReports::add, titusRuntime);
    }
}