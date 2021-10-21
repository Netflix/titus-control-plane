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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.JobStatus;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.tuple.Pair;
import org.junit.Test;
import org.pcollections.PMap;

import static com.netflix.titus.runtime.connector.jobmanager.snapshot.JobSnapshotTestUtil.newServiceJobWithTasks;
import static com.netflix.titus.runtime.connector.jobmanager.snapshot.JobSnapshotTestUtil.newServiceTask;
import static org.assertj.core.api.Assertions.assertThat;

public class CachedServiceJobTest {

    private final TitusRuntime titusRuntime = TitusRuntimes.test();

    @Test
    public void testInitial() {
        Pair<Job<ServiceJobExt>, PMap<String, Task>> jobAndTasks = newServiceJobWithTasks(0, 2, 1_000);
        Job<ServiceJobExt> job = jobAndTasks.getLeft();
        PMap<String, Task> tasks = jobAndTasks.getRight();

        CachedJob cached1 = CachedServiceJob.newServiceInstance(job, tasks, titusRuntime);
        assertThat(cached1.getJob()).isEqualTo(job);
        assertThat(cached1.getTasks()).containsAllEntriesOf(tasks);
    }

    @Test
    public void testUpdateJob() {
        PCollectionJobSnapshot initialSnapshot = initialSnapshot(2);
        CachedJob cached1 = CollectionsExt.first(initialSnapshot.cachedJobsById.values());

        Job<?> updatedJob = JobFunctions.changeJobStatus(cached1.getJob(), JobStatus.newBuilder().withState(JobState.KillInitiated).build());
        JobSnapshot updatedSnapshot = cached1.updateJob(initialSnapshot, updatedJob).orElse(null);
        assertThat(updatedSnapshot).isNotNull();
        assertThat(updatedSnapshot.getJobMap()).containsValue(updatedJob);
    }

    @Test
    public void testRemoveJob() {
        PCollectionJobSnapshot initialSnapshot = initialSnapshot(2);
        CachedJob cached1 = CollectionsExt.first(initialSnapshot.cachedJobsById.values());

        Job<?> updatedJob = JobFunctions.changeJobStatus(cached1.getJob(), JobStatus.newBuilder().withState(JobState.Finished).build());
        JobSnapshot updatedSnapshot = cached1.removeJob(initialSnapshot, updatedJob).orElse(null);
        assertThat(updatedSnapshot).isNotNull();
        assertThat(updatedSnapshot.getJobMap()).isEmpty();
        assertThat(updatedSnapshot.getTaskMap()).isEmpty();
    }

    @Test
    public void testAddTask() {
        PCollectionJobSnapshot initialSnapshot = initialSnapshot(2);

        // Empty
        CachedJob cached1 = CollectionsExt.first(initialSnapshot.cachedJobsById.values());
        assertThat(cached1.getTasks()).hasSize(2);

        // One task
        Task extraTask = newServiceTask((Job<ServiceJobExt>) cached1.getJob(), 3);
        PCollectionJobSnapshot withThreeTasks = (PCollectionJobSnapshot) cached1.updateTask(initialSnapshot, extraTask).orElse(null);
        CachedJob cached2 = CollectionsExt.first(withThreeTasks.cachedJobsById.values());
        assertThat(cached2.getTasks()).hasSize(3);
        assertThat(cached2.getTasks()).containsValue(extraTask);
    }

    @Test
    public void testUpdateTask() {
        PCollectionJobSnapshot initialSnapshot = initialSnapshot(2);
        CachedJob cached1 = CollectionsExt.first(initialSnapshot.cachedJobsById.values());

        Task someTask = CollectionsExt.first(cached1.getTasks().values());
        Task updatedTask = someTask.toBuilder().withStatus(TaskStatus.newBuilder().withState(TaskState.Started).build()).build();
        PCollectionJobSnapshot snapshot2 = (PCollectionJobSnapshot) cached1.updateTask(initialSnapshot, updatedTask).orElse(null);
        CachedJob cached2 = CollectionsExt.first(snapshot2.cachedJobsById.values());
        assertThat(cached2.getTasks()).hasSize(2);
        assertThat(cached2.getTasks().get(updatedTask.getId())).isEqualTo(updatedTask);
    }

    @Test
    public void testUpdateFinishedTask() {
        PCollectionJobSnapshot initialSnapshot = initialSnapshot(2);
        CachedJob cached1 = CollectionsExt.first(initialSnapshot.cachedJobsById.values());

        Task someTask = CollectionsExt.first(cached1.getTasks().values());
        Task updatedTask = someTask.toBuilder().withStatus(TaskStatus.newBuilder().withState(TaskState.Finished).build()).build();
        PCollectionJobSnapshot snapshot2 = (PCollectionJobSnapshot) cached1.updateTask(initialSnapshot, updatedTask).orElse(null);
        CachedJob cached2 = CollectionsExt.first(snapshot2.cachedJobsById.values());
        assertThat(cached2.getTasks()).hasSize(1);
    }

    @Test
    public void testRemoveTask() {
        PCollectionJobSnapshot initialSnapshot = initialSnapshot(2);
        CachedJob cached1 = CollectionsExt.first(initialSnapshot.cachedJobsById.values());

        Task someTask = CollectionsExt.first(cached1.getTasks().values());
        PCollectionJobSnapshot snapshot2 = (PCollectionJobSnapshot) cached1.removeTask(initialSnapshot, someTask).orElse(null);
        CachedJob cached2 = CollectionsExt.first(snapshot2.cachedJobsById.values());
        assertThat(cached2.getTasks()).hasSize(1);
    }

    private PCollectionJobSnapshot initialSnapshot(int taskCount) {
        Pair<Job<ServiceJobExt>, PMap<String, Task>> jobAndTasks = newServiceJobWithTasks(0, taskCount, 1_000);
        Job<ServiceJobExt> job = jobAndTasks.getLeft();
        Map<String, Task> tasks = new HashMap<>(jobAndTasks.getRight());

        return PCollectionJobSnapshot.newInstance(
                "test",
                Collections.singletonMap(job.getId(), job),
                Collections.singletonMap(job.getId(), tasks),
                false,
                error -> {
                    throw new IllegalStateException(error);
                },
                titusRuntime
        );
    }
}