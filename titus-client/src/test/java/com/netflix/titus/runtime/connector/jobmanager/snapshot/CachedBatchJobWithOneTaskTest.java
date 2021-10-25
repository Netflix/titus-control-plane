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

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.JobStatus;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.Version;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.tuple.Pair;
import org.junit.Test;
import org.pcollections.PMap;

import static com.netflix.titus.runtime.connector.jobmanager.snapshot.JobSnapshotTestUtil.newBatchJobWithTasks;
import static org.assertj.core.api.Assertions.assertThat;

public class CachedBatchJobWithOneTaskTest {

    private final TitusRuntime titusRuntime = TitusRuntimes.test();

    @Test
    public void testInitial() {
        Pair<Job<BatchJobExt>, PMap<String, Task>> jobAndTasks = newBatchJobWithTasks(0, 1);
        Job<BatchJobExt> job = jobAndTasks.getLeft();
        PMap<String, Task> tasks = jobAndTasks.getRight();

        // Create finished task for slot 0 to test filtering.
        Task task = CollectionsExt.first(tasks.values());
        Task finishedTask = task.toBuilder()
                .withId("finishedTaskId")
                .withStatus(TaskStatus.newBuilder().withState(TaskState.Finished).build())
                .withVersion(Version.newBuilder().withTimestamp(task.getVersion().getTimestamp() - 1_000).build())
                .build();
        tasks = tasks.plus(finishedTask.getId(), finishedTask);

        CachedJob cached1 = CachedBatchJobWithOneTask.newBatchInstance(job, tasks, titusRuntime);
        assertThat(cached1.getJob()).isEqualTo(job);
        assertThat(cached1.getTasks()).hasSize(1);
        assertThat(cached1.getTasks()).containsValue(task);
    }

    @Test
    public void testUpdateJob() {
        PCollectionJobSnapshot initialSnapshot = initialSnapshot(true).getLeft();
        CachedJob cached1 = CollectionsExt.first(initialSnapshot.cachedJobsById.values());

        Job<?> updatedJob = JobFunctions.changeJobStatus(cached1.getJob(), JobStatus.newBuilder().withState(JobState.KillInitiated).build());
        JobSnapshot updatedSnapshot = cached1.updateJob(initialSnapshot, updatedJob).orElse(null);
        assertThat(updatedSnapshot).isNotNull();
        assertThat(updatedSnapshot.getJobMap()).containsValue(updatedJob);
    }

    @Test
    public void testRemoveJob() {
        PCollectionJobSnapshot initialSnapshot = initialSnapshot(true).getLeft();
        CachedJob cached1 = CollectionsExt.first(initialSnapshot.cachedJobsById.values());

        Job<?> updatedJob = JobFunctions.changeJobStatus(cached1.getJob(), JobStatus.newBuilder().withState(JobState.Finished).build());
        JobSnapshot updatedSnapshot = cached1.removeJob(initialSnapshot, updatedJob).orElse(null);
        assertThat(updatedSnapshot).isNotNull();
        assertThat(updatedSnapshot.getJobMap()).isEmpty();
        assertThat(updatedSnapshot.getTaskMap()).isEmpty();
    }

    @Test
    public void testRemoveJobWithoutTask() {
        PCollectionJobSnapshot initialSnapshot = initialSnapshot(false).getLeft();
        CachedJob cached1 = CollectionsExt.first(initialSnapshot.cachedJobsById.values());
        assertThat(cached1.getTasks()).isEmpty();

        JobSnapshot updatedSnapshot = cached1.removeJob(initialSnapshot, cached1.getJob()).orElse(null);
        assertThat(updatedSnapshot).isNotNull();
        assertThat(updatedSnapshot.getJobMap()).hasSize(0);
        assertThat(updatedSnapshot.getTaskMap()).hasSize(0);
    }

    @Test
    public void testAddTask() {
        Pair<PCollectionJobSnapshot, Task> initial = initialSnapshot(false);
        PCollectionJobSnapshot initialSnapshot = initial.getLeft();
        Task task = initial.getRight();

        // Empty
        CachedJob cached1 = CollectionsExt.first(initialSnapshot.cachedJobsById.values());
        assertThat(cached1.getTasks()).isEmpty();

        // One task
        PCollectionJobSnapshot withOneTask = (PCollectionJobSnapshot) cached1.updateTask(initialSnapshot, task).orElse(null);
        CachedJob cached2 = CollectionsExt.first(withOneTask.cachedJobsById.values());
        assertThat(cached2.getTasks()).containsValue(task);
    }

    @Test
    public void testUpdateTask() {
        Pair<PCollectionJobSnapshot, Task> initial = initialSnapshot(true);
        PCollectionJobSnapshot initialSnapshot = initial.getLeft();
        Task task = initial.getRight();

        Task task0Updated = task.toBuilder().withStatus(TaskStatus.newBuilder().withState(TaskState.Started).build()).build();
        CachedJob cached1 = CollectionsExt.first(initialSnapshot.cachedJobsById.values());

        PCollectionJobSnapshot snapshot2 = (PCollectionJobSnapshot) cached1.updateTask(initialSnapshot, task0Updated).orElse(null);
        CachedJob cached2 = CollectionsExt.first(snapshot2.cachedJobsById.values());

        assertThat(cached2.getTasks()).hasSize(1);
        assertThat(cached2.getTasks().get(task0Updated.getId())).isEqualTo(task0Updated);
    }

    private Pair<PCollectionJobSnapshot, Task> initialSnapshot(boolean create) {
        Pair<Job<BatchJobExt>, PMap<String, Task>> jobAndTasks = newBatchJobWithTasks(0, 1);
        Job<BatchJobExt> job = jobAndTasks.getLeft();
        Task task = CollectionsExt.first(jobAndTasks.getRight().values());

        PCollectionJobSnapshot snapshot = PCollectionJobSnapshot.newInstance("test",
                Collections.singletonMap(job.getId(), job),
                Collections.singletonMap(job.getId(), create ? Collections.singletonMap(task.getId(), task) : Collections.emptyMap()),
                false,
                error -> {
                    throw new IllegalStateException(error);
                },
                titusRuntime
        );
        return Pair.of(snapshot, task);
    }
}