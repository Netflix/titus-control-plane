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
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Stopwatch;
import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.JobStatus;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.Version;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;
import org.pcollections.PSequence;
import org.pcollections.TreePVector;

/**
 * Performance test for {@link PCollectionJobSnapshot}.
 */
public class JobSnapshotPerf {

    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    private final int taskPerJobCount;
    private final double createUpdateRatio;

    private final AtomicLong jobIdx = new AtomicLong();
    private final AtomicLong taskIdx = new AtomicLong();
    private final Random random = new Random();

    private JobSnapshot snapshot;
    private PSequence<Pair<Job<?>, PMap<String, Task>>> jobAndTasks = TreePVector.empty();

    public JobSnapshotPerf(int jobCount, int taskPerJobCount, double createUpdateRatio) {
        this.taskPerJobCount = taskPerJobCount;
        this.createUpdateRatio = createUpdateRatio;
        Map<String, Job<?>> jobs = new HashMap<>();
        Map<String, Map<String, Task>> taskByJobId = new HashMap<>();
        for (int j = 0; j < jobCount; j++) {
            Pair<Job<?>, Map<String, Task>> jobWithTasks = newJobWithTasks();
            Job<?> job = jobWithTasks.getLeft();
            Map<String, Task> tasks = jobWithTasks.getRight();

            jobs.put(job.getId(), job);
            taskByJobId.put(job.getId(), tasks);
            jobAndTasks = jobAndTasks.plus(Pair.of(job, HashTreePMap.from(tasks)));
        }
        this.snapshot = PCollectionJobSnapshot.newInstance(
                "test", jobs, taskByJobId, false, false, message -> {
                },
                titusRuntime
        );
    }

    private Pair<Job<?>, Map<String, Task>> newJobWithTasks() {
        Job<BatchJobExt> job = JobGenerator.oneBatchJob().toBuilder().withId("job#" + jobIdx.getAndIncrement()).build();
        Map<String, Task> tasks = new HashMap<>();
        for (int t = 0; t < taskPerJobCount; t++) {
            BatchJobTask task = newTask(job);
            tasks.put(task.getId(), task);
        }
        return Pair.of(job, tasks);
    }

    private BatchJobTask newTask(Job<?> job) {
        return JobGenerator.oneBatchTask().toBuilder()
                .withId("task#" + taskIdx.getAndIncrement() + "@" + job.getId())
                .withJobId(job.getId())
                .withVersion(Version.newBuilder().withTimestamp(System.currentTimeMillis()).build())
                .build();
    }

    private void run(long updateCount) {
        long createActions = 1;
        long updateActions = 1;
        for (int i = 0; i < updateCount; i++) {
            boolean doJob = random.nextBoolean();
            if ((createUpdateRatio * createActions) < updateActions) {
                if (doJob) {
                    createJob();
                } else {
                    createTask();
                }
                createActions++;
            } else {
                if (doJob) {
                    updateJob();
                } else {
                    updateTask();
                }
                updateActions++;
            }
        }
    }

    private void createJob() {
        Job<?> toRemove = jobAndTasks.get(0).getLeft();

        Pair<Job<?>, Map<String, Task>> toAdd = newJobWithTasks();
        Job<?> toAddJob = toAdd.getLeft();
        Map<String, Task> toAddTasks = toAdd.getRight();

        // Remove old job by moving it to the finished state.
        Job<?> finishedJob = toRemove.toBuilder().withStatus(JobStatus.newBuilder().withState(JobState.Finished).build()).build();
        snapshot.updateJob(finishedJob).ifPresent(newSnapshot -> this.snapshot = newSnapshot);

        // Add new job as a replacement
        snapshot.updateJob(toAddJob).ifPresent(newSnapshot -> this.snapshot = newSnapshot);
        toAddTasks.forEach((taskId, task) ->
                snapshot.updateTask(task, false).ifPresent(newSnapshot -> this.snapshot = newSnapshot)
        );

        // Clean local map
        jobAndTasks = jobAndTasks.minus(0).plus(Pair.of(toAddJob, HashTreePMap.from(toAddTasks)));
    }

    private void updateJob() {
        int idx = random.nextInt(jobAndTasks.size());
        Pair<Job<?>, PMap<String, Task>> jobToUpdate = jobAndTasks.get(idx);
        Job<?> updatedJob = jobToUpdate.getLeft().toBuilder().withVersion(Version.newBuilder().withTimestamp(System.currentTimeMillis()).build()).build();

        snapshot.updateJob(updatedJob).ifPresent(newSnapshot -> this.snapshot = newSnapshot);

        // Clean local map
        jobAndTasks = jobAndTasks.with(idx, Pair.of(updatedJob, jobToUpdate.getRight()));
    }

    private void createTask() {
        int idx = random.nextInt(jobAndTasks.size());
        Pair<Job<?>, PMap<String, Task>> toUpdate = this.jobAndTasks.get(idx);

        Job<?> job = toUpdate.getLeft();
        PMap<String, Task> tasks = toUpdate.getRight();

        // Remove task
        int taskIdx = random.nextInt(tasks.size());
        String taskToRemoveId = new ArrayList<>(tasks.values()).get(taskIdx).getId();
        Task taskToRemove = snapshot.getTaskMap().get(taskToRemoveId);
        if (taskToRemove == null) {
            return;
        }
        Task finishedTask = taskToRemove.toBuilder().withStatus(TaskStatus.newBuilder().withState(TaskState.Finished).build()).build();
        snapshot.updateTask(finishedTask, false).ifPresent(newSnapshot -> this.snapshot = newSnapshot);

        // Create replacement
        Task newTask = newTask(job);
        snapshot.updateTask(newTask, false).ifPresent(newSnapshot -> this.snapshot = newSnapshot);

        // Clean local map
        jobAndTasks = jobAndTasks.with(idx, Pair.of(job, tasks.minus(taskIdx).plus(newTask.getId(), newTask)));
    }

    private void updateTask() {
        int idx = random.nextInt(jobAndTasks.size());
        Pair<Job<?>, PMap<String, Task>> toUpdate = this.jobAndTasks.get(idx);

        Job<?> job = toUpdate.getLeft();
        PMap<String, Task> tasks = toUpdate.getRight();

        int taskIdx = random.nextInt(tasks.size());
        Task taskToUpdate = new ArrayList<>(tasks.values()).get(taskIdx);
        Task updatedTask = taskToUpdate.toBuilder().withVersion(Version.newBuilder().withTimestamp(System.currentTimeMillis()).build()).build();
        snapshot.updateTask(updatedTask, false).ifPresent(newSnapshot -> this.snapshot = newSnapshot);

        // Clean local map
        jobAndTasks = jobAndTasks.with(idx, Pair.of(job, tasks.plus(updatedTask.getId(), updatedTask)));
    }

    public static void main(String[] args) {
        JobSnapshotPerf perf = new JobSnapshotPerf(5000, 1, 0.5);
        Stopwatch stopwatch = Stopwatch.createStarted();
        perf.run(10_000);
        System.out.println("Finished in: " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + "[ms]");
    }
}