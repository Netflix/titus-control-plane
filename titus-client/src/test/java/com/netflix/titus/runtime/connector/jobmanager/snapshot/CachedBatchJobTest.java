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
import java.util.Iterator;
import java.util.Map;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.JobStatus;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.common.util.tuple.Triple;
import org.junit.Test;
import org.pcollections.PMap;

import static com.netflix.titus.runtime.connector.jobmanager.snapshot.JobSnapshotTestUtil.newBatchJobWithTasks;
import static org.assertj.core.api.Assertions.assertThat;

public class CachedBatchJobTest {

    private final TitusRuntime titusRuntime = TitusRuntimes.test();

    @Test
    public void testInitial() {
        Pair<Job<BatchJobExt>, PMap<String, Task>> jobAndTasks = newBatchJobWithTasks(0, 2);
        Job<BatchJobExt> job = jobAndTasks.getLeft();
        PMap<String, Task> tasks = jobAndTasks.getRight();

        CachedJob cached1 = CachedBatchJob.newBatchInstance(job, tasks, titusRuntime);
        assertThat(cached1.getJob()).isEqualTo(job);
        assertThat(cached1.getTasks()).containsAllEntriesOf(tasks);
    }

    @Test
    public void testUpdateJob() {
        PCollectionJobSnapshot initialSnapshot = initialSnapshot(2, 2).getFirst();
        CachedJob cached1 = CollectionsExt.first(initialSnapshot.cachedJobsById.values());

        Job<?> updatedJob = JobFunctions.changeJobStatus(cached1.getJob(), JobStatus.newBuilder().withState(JobState.KillInitiated).build());
        JobSnapshot updatedSnapshot = cached1.updateJob(initialSnapshot, updatedJob).orElse(null);
        assertThat(updatedSnapshot).isNotNull();
        assertThat(updatedSnapshot.getJobMap()).containsValue(updatedJob);
    }

    @Test
    public void testRemoveJob() {
        PCollectionJobSnapshot initialSnapshot = initialSnapshot(2, 2).getFirst();
        CachedJob cached1 = CollectionsExt.first(initialSnapshot.cachedJobsById.values());

        Job<?> updatedJob = JobFunctions.changeJobStatus(cached1.getJob(), JobStatus.newBuilder().withState(JobState.Finished).build());
        JobSnapshot updatedSnapshot = cached1.removeJob(initialSnapshot, updatedJob).orElse(null);
        assertThat(updatedSnapshot).isNotNull();
        assertThat(updatedSnapshot.getJobMap()).isEmpty();
        assertThat(updatedSnapshot.getTaskMap()).isEmpty();
    }

    @Test
    public void testAddTask() {
        Triple<PCollectionJobSnapshot, Map<Integer, Task>, Map<Integer, Task>> initial = initialSnapshot(2, 0);
        PCollectionJobSnapshot initialSnapshot = initial.getFirst();
        Map<Integer, Task> skippedTasks = initial.getThird();

        // Empty
        CachedJob cached1 = CollectionsExt.first(initialSnapshot.cachedJobsById.values());
        assertThat(cached1.getTasks()).isEmpty();

        // One task
        PCollectionJobSnapshot withOneTask = (PCollectionJobSnapshot) cached1.updateTask(initialSnapshot, skippedTasks.get(0)).orElse(null);
        CachedJob cached2 = CollectionsExt.first(withOneTask.cachedJobsById.values());
        assertThat(cached2.getTasks()).containsValue(skippedTasks.get(0));

        // Two tasks
        PCollectionJobSnapshot withTwoTasks = (PCollectionJobSnapshot) cached2.updateTask(withOneTask, skippedTasks.get(1)).orElse(null);
        CachedJob cached3 = CollectionsExt.first(withTwoTasks.cachedJobsById.values());
        assertThat(cached3.getTasks()).hasSize(2);
        assertThat(cached3.getTasks()).containsValue(skippedTasks.get(1));
    }

    @Test
    public void testUpdateTask() {
        Triple<PCollectionJobSnapshot, Map<Integer, Task>, Map<Integer, Task>> initial = initialSnapshot(2, 2);
        PCollectionJobSnapshot initialSnapshot = initial.getFirst();
        Map<Integer, Task> tasksByIndexes = initial.getSecond();
        CachedJob cached1 = CollectionsExt.first(initialSnapshot.cachedJobsById.values());
        assertThat(cached1.getTasks()).hasSize(2);

        // Update task at index 0
        Task task0Updated = tasksByIndexes.get(0).toBuilder().withStatus(TaskStatus.newBuilder().withState(TaskState.Started).build()).build();
        PCollectionJobSnapshot snapshot2 = (PCollectionJobSnapshot) cached1.updateTask(initialSnapshot, task0Updated).orElse(null);
        CachedJob cached2 = CollectionsExt.first(snapshot2.cachedJobsById.values());
        assertThat(cached2.getTasks()).hasSize(2);
        assertThat(cached2.getTasks().get(task0Updated.getId())).isEqualTo(task0Updated);
    }

    /**
     * Create a snapshot with a single job and size == taskCount. Create up to tasksCreated tasks, leaving
     * the remaining slots empty.
     */
    private Triple<PCollectionJobSnapshot, Map<Integer, Task>, Map<Integer, Task>> initialSnapshot(int taskCount, int tasksCreated) {
        Pair<Job<BatchJobExt>, PMap<String, Task>> jobAndTasks = newBatchJobWithTasks(0, taskCount);
        Job<BatchJobExt> job = jobAndTasks.getLeft();
        Map<Integer, Task> tasksByIndex = new HashMap<>();
        Map<String, Task> tasks = new HashMap<>(jobAndTasks.getRight());
        Map<Integer, Task> skipped = new HashMap<>();
        Iterator<Task> it = tasks.values().iterator();
        while (it.hasNext()) {
            Task task = it.next();
            int index = CachedBatchJob.indexOf(task, taskCount, titusRuntime);
            if (index >= tasksCreated) {
                it.remove();
                skipped.put(index, task);
            } else {
                tasksByIndex.put(index, task);
            }
        }

        PCollectionJobSnapshot snapshot = PCollectionJobSnapshot.newInstance("test",
                Collections.singletonMap(job.getId(), job),
                Collections.singletonMap(job.getId(), tasks),
                false,
                false,
                error -> {
                    throw new IllegalStateException(error);
                },
                titusRuntime
        );
        return Triple.of(snapshot, tasksByIndex, skipped);
    }
}