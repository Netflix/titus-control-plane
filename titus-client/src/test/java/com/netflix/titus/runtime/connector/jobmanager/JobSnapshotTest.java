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

package com.netflix.titus.runtime.connector.jobmanager;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.JobStatus;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.Version;
import com.netflix.titus.common.util.tuple.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.netflix.titus.runtime.connector.jobmanager.JobSnapshotTestUtil.newJobWithTasks;
import static com.netflix.titus.runtime.connector.jobmanager.JobSnapshotTestUtil.newSnapshot;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Parameterized.class)
public class JobSnapshotTest {

    private final JobSnapshotFactory factory;

    @Parameterized.Parameters
    public static List<JobSnapshotFactory> factories() {
        return Arrays.asList(JobSnapshotFactories.newDefault(), JobSnapshotFactories.newLegacy());
    }

    public JobSnapshotTest(JobSnapshotFactory factory) {
        this.factory = factory;
    }

    @Test
    public void testInitialization() {
        Pair<Job<?>, List<Task>> pair1 = newJobWithTasks(1, 5);
        Pair<Job<?>, List<Task>> pair2 = newJobWithTasks(2, 10);
        Job<?> job1 = pair1.getLeft();
        Job<?> job2 = pair2.getLeft();
        List<Task> tasks1 = pair1.getRight();
        List<Task> tasks2 = pair2.getRight();
        JobSnapshot jobSnapshot = newSnapshot(factory, pair1, pair2);

        // getJobMap()
        assertThat(jobSnapshot.getJobMap()).containsKeys(job1.getId(), job2.getId());

        // getTasks(<jobId>)
        assertThat(jobSnapshot.getTasks(job1.getId())).containsAll(tasks1);
        assertThat(jobSnapshot.getTasks(job2.getId())).containsAll(tasks2);

        // getTasksMap()
        assertThat(jobSnapshot.getTaskMap().values()).containsAll(tasks1).containsAll(tasks2);

        // ------------------------------------------------------
        // DEPRECATED

        // getJobs()
        assertThat(jobSnapshot.getJobs()).contains(job1, job2);

        // getTasks()
        assertThat(jobSnapshot.getTasks()).containsAll(tasks1).containsAll(tasks2);

        // getJobsAndTasks()
        assertThat(jobSnapshot.getJobsAndTasks()).contains(Pair.of(job1, tasks1)).contains(Pair.of(job2, tasks2));
    }

    @Test
    public void testJobAndTaskUpdate() {
        Pair<Job<?>, List<Task>> pair1 = newJobWithTasks(1, 2);
        Pair<Job<?>, List<Task>> pair2 = newJobWithTasks(2, 2);
        Job<?> job1 = pair1.getLeft();
        Job<?> job2 = pair2.getLeft();
        List<Task> tasks1 = pair1.getRight();
        List<Task> tasks2 = pair2.getRight();
        JobSnapshot initial = newSnapshot(factory, pair1);

        // Add job2
        JobSnapshot updated = initial.updateJob(job2).orElse(null);
        assertThat(updated).isNotNull();
        assertThat(updated.getJobMap()).containsValues(job1, job2);

        // Add tasks of job2
        updated = updated.updateTask(tasks2.get(0), false).orElse(null);
        assertThat(updated).isNotNull();
        updated = updated.updateTask(tasks2.get(1), false).orElse(null);
        assertThat(updated).isNotNull();
        assertThat(updated.getTasks(job2.getId())).containsAll(tasks2);
        assertThat(updated.getTaskMap()).hasSize(4);

        // Modify job1
        Job<?> updatedJob = job1.toBuilder().withVersion(Version.newBuilder().withTimestamp(123).build()).build();
        updated = updated.updateJob(updatedJob).orElse(null);
        assertThat(updated).isNotNull();
        assertThat(updated.getJobMap()).containsValues(updatedJob, job2);

        // Modify task (job1)
        Task updatedTask = tasks1.get(0).toBuilder().withVersion(Version.newBuilder().withTimestamp(123).build()).build();
        updated = updated.updateTask(updatedTask, false).orElse(null);
        assertThat(updated).isNotNull();
        assertThat(updated.getTasks(job1.getId())).hasSize(2);
        assertThat(updated.getTasks(job1.getId())).contains(tasks1.get(1)).contains(updatedTask);
        assertThat(updated.getTaskMap()).hasSize(4);
        assertThat(updated.getTaskMap().get(updatedTask.getId())).isEqualTo(updatedTask);

        // Remove task (job1)
        updated = updated.updateTask(
                updatedTask.toBuilder().withStatus(TaskStatus.newBuilder().withState(TaskState.Finished).build()).build(), false
        ).orElse(null);
        assertThat(updated).isNotNull();
        assertThat(updated.getTasks(job1.getId())).containsExactly(tasks1.get(1));
        assertThat(updated.getTaskMap()).hasSize(3);

        // Remove job1
        updated = updated.updateJob(
                updatedJob.toBuilder().withStatus(JobStatus.newBuilder().withState(JobState.Finished).build()).build()
        ).orElse(null);
        assertThat(updated).isNotNull();
        assertThat(updated.getJobMap()).hasSize(1).containsEntry(job2.getId(), job2);
        assertThat(updated.getTasks(job2.getId())).containsAll(tasks2);
        assertThat(updated.getTaskMap()).hasSize(2).containsValues(tasks2.get(0), tasks2.get(1));
    }

    @Test
    public void testMovedTask() {
        Pair<Job<?>, List<Task>> pair1 = newJobWithTasks(1, 2);
        Pair<Job<?>, List<Task>> pair2 = newJobWithTasks(2, 0);
        Job<?> job1 = pair1.getLeft();
        Job<?> job2 = pair2.getLeft();
        List<Task> tasks1 = pair1.getRight();
        JobSnapshot initial = newSnapshot(factory, pair1, pair2);

        Task movedTask = tasks1.get(0).toBuilder()
                .withJobId(job2.getId())
                .withTaskContext(Collections.singletonMap(TaskAttributes.TASK_ATTRIBUTES_MOVED_FROM_JOB, job1.getId()))
                .build();
        JobSnapshot updated = initial.updateTask(movedTask, true).orElse(null);
        assertThat(updated).isNotNull();
        assertThat(updated.getJobMap()).hasSize(2).containsValues(job1, job2);
        assertThat(updated.getTaskMap()).containsValues(movedTask, tasks1.get(1));
        assertThat(updated.getTasks(job1.getId())).containsExactly(tasks1.get(1));
        assertThat(updated.getTasks(job2.getId())).containsExactly(movedTask);
    }
}
