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

package com.netflix.titus.runtime.jobmanager;

import java.util.Arrays;
import java.util.List;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.JobStatus;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.model.IdAndTimestampKey;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JobComparatorsTest {

    @Test
    public void testGetJobTimestampComparator() {
        List<Job<?>> jobs = Arrays.asList(
                newJob("job3", 10),
                newJob("job2", 5),
                newJob("job1", 10)
        );
        jobs.sort(JobComparators.getJobTimestampComparator());

        assertThat(jobs.get(0).getId()).isEqualTo("job2");
        assertThat(jobs.get(1).getId()).isEqualTo("job1");
        assertThat(jobs.get(2).getId()).isEqualTo("job3");
    }

    @Test
    public void testGetTaskTimestampComparator() {
        List<Task> tasks = Arrays.asList(
                newTask("task3", 10),
                newTask("task2", 5),
                newTask("task1", 10)
        );
        tasks.sort(JobComparators.getTaskTimestampComparator());

        assertThat(tasks.get(0).getId()).isEqualTo("task2");
        assertThat(tasks.get(1).getId()).isEqualTo("task1");
        assertThat(tasks.get(2).getId()).isEqualTo("task3");
    }

    @Test
    public void testCreateJobKeyOf() {
        IdAndTimestampKey<Job<?>> key = JobComparators.createJobKeyOf(newJob("job1", 100));
        assertThat(key.getId()).isEqualTo("job1");
        assertThat(key.getTimestamp()).isEqualTo(100);
    }

    @Test
    public void testCreateTaskKeyOf() {
        IdAndTimestampKey<Task> key = JobComparators.createTaskKeyOf(newTask("task1", 100));
        assertThat(key.getId()).isEqualTo("task1");
        assertThat(key.getTimestamp()).isEqualTo(100);
    }

    private Job<?> newJob(String jobId, long timestamp) {
        return JobGenerator.oneBatchJob().toBuilder()
                .withId(jobId)
                .withStatus(JobStatus.newBuilder()
                        .withState(JobState.Accepted)
                        .withTimestamp(timestamp)
                        .build()
                )
                .build();
    }

    private Task newTask(String taskId, long timestamp) {
        return JobGenerator.oneBatchTask().toBuilder()
                .withId(taskId)
                .withStatus(TaskStatus.newBuilder()
                        .withState(TaskState.Accepted)
                        .withTimestamp(timestamp)
                        .build()
                )
                .build();
    }
}