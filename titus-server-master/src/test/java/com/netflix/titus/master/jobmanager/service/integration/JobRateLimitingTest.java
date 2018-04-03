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

package com.netflix.titus.master.jobmanager.service.integration;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor.JobDescriptorExt;
import com.netflix.titus.api.jobmanager.model.job.JobModel;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.master.jobmanager.service.integration.scenario.JobScenarioBuilder;
import com.netflix.titus.master.jobmanager.service.integration.scenario.JobsScenarioBuilder;
import com.netflix.titus.master.jobmanager.service.integration.scenario.ScenarioTemplates;
import org.junit.Test;

import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.changeBatchJobSize;
import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.changeRetryPolicy;
import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.changeServiceJobCapacity;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskServiceJobDescriptor;
import static org.assertj.core.api.Assertions.assertThat;

public class JobRateLimitingTest {

    private static final long RETREY_DELAY_MS = 1_000;

    private static final JobDescriptor<BatchJobExt> LARGE_BATCH_JOB = changeRetryPolicy(
            changeBatchJobSize(oneTaskBatchJobDescriptor(), 3 * JobsScenarioBuilder.ACTIVE_NOT_STARTED_TASKS_LIMIT),
            JobModel.newDelayedRetryPolicy().withDelay(RETREY_DELAY_MS, TimeUnit.MILLISECONDS).withRetries(1).build()
    );

    private static final JobDescriptor<ServiceJobExt> LARGE_SERVIE_JOB = changeRetryPolicy(
            changeServiceJobCapacity(oneTaskServiceJobDescriptor(), 3 * JobsScenarioBuilder.ACTIVE_NOT_STARTED_TASKS_LIMIT),
            JobModel.newDelayedRetryPolicy().withDelay(RETREY_DELAY_MS, TimeUnit.MILLISECONDS).withRetries(1).build()
    );

    private final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder();

    /**
     * Run a large job, and check that tasks are started in partitions, not all at once.
     */
    @Test
    public void testLargeBatchJobRateLimiting() throws Exception {
        testLargeServiceJobRateLimiting(LARGE_BATCH_JOB);
    }

    @Test
    public void testLargeServiceJobRateLimiting() throws Exception {
        testLargeServiceJobRateLimiting(LARGE_SERVIE_JOB);
    }

    private void testLargeServiceJobRateLimiting(JobDescriptor<?> jobDescriptor) throws Exception {
        jobsScenarioBuilder.scheduleJob(jobDescriptor, jobScenario -> jobScenario
                .expectJobEvent()
                .advance().allTasks(tasks -> acceptAndStartNewPartition(jobScenario, tasks))
                .advance().allTasks(tasks -> acceptAndStartNewPartition(jobScenario, tasks))
                .advance().allTasks(tasks -> acceptAndStartNewPartition(jobScenario, tasks))
                .advance().allTasks(this::expectAllStarted)
        );
    }

    /**
     * Run a large job with tasks immediately failing. Check that first batch is rescheduled periodically.
     */
    @Test
    public void testLargeBatchJobWithFailingTasksRateLimiting() throws Exception {
        testLargeBatchJobWithFailingTasksRateLimiting(LARGE_BATCH_JOB);
    }

    @Test
    public void testLargeServiceJobWithFailingTasksRateLimiting() throws Exception {
        testLargeBatchJobWithFailingTasksRateLimiting(LARGE_SERVIE_JOB);
    }

    private void testLargeBatchJobWithFailingTasksRateLimiting(JobDescriptor<?> jobDescriptor) {
        jobsScenarioBuilder.scheduleJob(jobDescriptor, jobScenario -> jobScenario
                .expectJobEvent()
                // First batch fails, and is retried
                .advance().allTasks(tasks -> acceptAndFailNewPartition(jobScenario, tasks))
                .advance().allTasks(tasks -> startNewBatch(jobScenario, tasks))
                // Second batch succeeds immediately
                .advance().allTasks(tasks -> acceptAndStartNewPartition(jobScenario, tasks))
                // Third batch fails again
                .advance().allTasks(tasks -> acceptAndFailNewPartition(jobScenario, tasks))
                .advance().allTasks(tasks -> startNewBatch(jobScenario, tasks))
                .advance().allTasks(this::expectAllStarted)
        );
    }

    /**
     * Run all tasks of a large job. Fail them all, and check that they are rescheduled in partitions.
     */
    @Test
    public void testLargeBatchJobTaskRetryRateLimiting() {
        testLargeServiceJobTaskRetryRateLimiting(LARGE_BATCH_JOB);
    }

    @Test
    public void testLargeServiceJobTaskRetryRateLimiting() {
        testLargeServiceJobTaskRetryRateLimiting(LARGE_SERVIE_JOB);
    }

    private void testLargeServiceJobTaskRetryRateLimiting(JobDescriptor<?> jobDescriptor) {
        jobsScenarioBuilder.scheduleJob(jobDescriptor, jobScenario -> jobScenario
                .expectJobEvent()
                // Start all tasks
                .advance().allTasks(tasks -> acceptAndStartNewPartition(jobScenario, tasks))
                .advance().allTasks(tasks -> acceptAndStartNewPartition(jobScenario, tasks))
                .advance().allTasks(tasks -> acceptAndStartNewPartition(jobScenario, tasks))
                // Fail all tasks
                .advance().allTasks(tasks -> tasks.forEach(task -> jobScenario.triggerMesosFinishedEvent(task, -1, TaskStatus.REASON_FAILED)))
                .advance(1, TimeUnit.SECONDS)
                // Expect all be restarted in partitions
                .advance().allTasks(tasks -> acceptAndStartNewPartition(jobScenario, tasks))
                .advance().allTasks(tasks -> acceptAndStartNewPartition(jobScenario, tasks))
                .advance().allTasks(tasks -> acceptAndStartNewPartition(jobScenario, tasks))
                .advance().allTasks(this::expectAllStarted)
        );
    }

    private <E extends JobDescriptorExt> void acceptAndStartNewPartition(JobScenarioBuilder<E> jobScenario, List<Task> tasks) {
        jobScenario.inAllTasks(filterActiveTasks(tasks, JobsScenarioBuilder.ACTIVE_NOT_STARTED_TASKS_LIMIT), (taskIdx, resubmit) ->
                jobScenario
                        .template(ScenarioTemplates.acceptTask(taskIdx, resubmit))
                        .template(ScenarioTemplates.startTask(taskIdx, resubmit, TaskState.Started))
        );
    }

    private <E extends JobDescriptorExt> void acceptAndFailNewPartition(JobScenarioBuilder<E> jobScenario, List<Task> tasks) {
        jobScenario.inAllTasks(filterActiveTasks(tasks, JobsScenarioBuilder.ACTIVE_NOT_STARTED_TASKS_LIMIT), (taskIdx, resubmit) ->
                jobScenario
                        .template(ScenarioTemplates.acceptTask(taskIdx, resubmit))
                        .template(ScenarioTemplates.failRetryableTask(taskIdx, resubmit, RETREY_DELAY_MS))
        );
    }

    private <E extends JobDescriptorExt> void startNewBatch(JobScenarioBuilder<E> jobScenario, List<Task> tasks) {
        jobScenario.inAllTasks(filterActiveTasks(tasks, JobsScenarioBuilder.ACTIVE_NOT_STARTED_TASKS_LIMIT), (taskIdx, resubmit) ->
                jobScenario
                        .template(ScenarioTemplates.startTask(taskIdx, resubmit, TaskState.Started))
        );
    }

    private void expectAllStarted(List<Task> tasks) {
        assertThat(tasks).hasSize(3 * JobsScenarioBuilder.ACTIVE_NOT_STARTED_TASKS_LIMIT);
        assertThat(tasks.stream().filter(t -> t.getStatus().getState() == TaskState.Started)).hasSize(3 * JobsScenarioBuilder.ACTIVE_NOT_STARTED_TASKS_LIMIT);
    }

    private List<Task> filterActiveTasks(List<Task> tasks, int expected) {
        List<Task> active = tasks.stream().filter(t -> t.getStatus().getState() == TaskState.Accepted).collect(Collectors.toList());
        assertThat(active).describedAs("Expected to find %d active tasks, but is %d", expected, active.size()).hasSize(expected);
        return active;
    }
}
