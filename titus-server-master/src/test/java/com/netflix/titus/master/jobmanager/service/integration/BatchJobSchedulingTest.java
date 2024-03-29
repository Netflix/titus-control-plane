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

import java.util.concurrent.TimeUnit;

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.JobModel;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.api.jobmanager.service.JobManagerException.ErrorCode;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.master.jobmanager.service.integration.scenario.JobsScenarioBuilder;
import com.netflix.titus.master.jobmanager.service.integration.scenario.ScenarioTemplates;
import org.junit.Test;

import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.changeBatchJobSize;
import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.changeRetryLimit;
import static com.netflix.titus.master.jobmanager.service.integration.scenario.JobsScenarioBuilder.CONCURRENT_STORE_UPDATE_LIMIT;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;
import static org.assertj.core.api.Assertions.assertThat;

public class BatchJobSchedulingTest {

    private final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder();

    /**
     * Run single batch task that terminates with exit code 0.
     */
    @Test
    public void testRunAndCompleteOkOneJobTask() {
        jobsScenarioBuilder.scheduleJob(oneTaskBatchJobDescriptor(), jobScenario -> jobScenario
                .template(ScenarioTemplates.acceptJobWithOneTask(0, 0))
                .template(ScenarioTemplates.startTask(0, 0, TaskState.Started))
                .template(ScenarioTemplates.finishSingleTaskJob(0, 0, TaskStatus.REASON_NORMAL, 0))
        );
    }

    /**
     * Run multiple batch tasks that terminates with exit code 0.
     */
    @Test
    public void testRunAndCompleteOkJobWithManyTasks() {
        JobDescriptor<BatchJobExt> twoTaskJob = changeBatchJobSize(oneTaskBatchJobDescriptor(), 2);
        jobsScenarioBuilder.scheduleJob(twoTaskJob, jobScenario -> jobScenario
                .expectJobEvent()
                .advance()
                .inActiveTasks((taskIdx, resubmit) -> ScenarioTemplates.acceptTask(taskIdx, resubmit))
                .inActiveTasks((taskIdx, resubmit) -> ScenarioTemplates.startTask(taskIdx, resubmit, TaskState.Started))
                .inActiveTasks((taskIdx, resubmit) -> ScenarioTemplates.triggerComputeProviderFinishedEvent(taskIdx, resubmit, 0))
                .template(ScenarioTemplates.verifyJobWithFinishedTasksCompletes())
        );
    }

    /**
     * Check that containers terminating with exit code 0 are not restarted.
     */
    @Test
    public void testTaskCompletedOkIsNotRestarted() {
        JobDescriptor<BatchJobExt> jobWithOneRetry = changeRetryLimit(oneTaskBatchJobDescriptor(), 1);
        jobsScenarioBuilder.scheduleJob(jobWithOneRetry, jobScenario -> jobScenario
                .template(ScenarioTemplates.acceptJobWithOneTask(0, 0))
                .template(ScenarioTemplates.startTask(0, 0, TaskState.Started))
                .template(ScenarioTemplates.finishSingleTaskJob(0, 0, TaskStatus.REASON_NORMAL, 0))
        );
    }

    /**
     * Check that containers that fail with non zero exit code, and are not retryable, are not restarted.
     */
    @Test
    public void testFailedTaskWithNoRetriesFinishesImmediately() {
        jobsScenarioBuilder.scheduleJob(oneTaskBatchJobDescriptor(), jobScenario -> jobScenario
                .template(ScenarioTemplates.acceptJobWithOneTask(0, 0))
                .template(ScenarioTemplates.startTask(0, 0, TaskState.Started))
                .template(ScenarioTemplates.finishSingleTaskJob(0, 0, TaskStatus.REASON_FAILED, -1))
        );
    }

    /**
     * Check container restart for failing containers that are retryable.
     */
    @Test
    public void testFailedTaskWithRetriesIsResubmitted() {
        JobDescriptor<BatchJobExt> jobWithOneRetry = changeRetryLimit(oneTaskBatchJobDescriptor(), 1);
        jobsScenarioBuilder.scheduleJob(jobWithOneRetry, jobScenario -> jobScenario
                .template(ScenarioTemplates.acceptJobWithOneTask(0, 0))
                .template(ScenarioTemplates.startTask(0, 0, TaskState.Started))
                .template(ScenarioTemplates.failRetryableTask(0, 0))
                .template(ScenarioTemplates.failLastBatchRetryableTask(0, 1))
        );
    }

    /**
     * Check that in a job with multiple tasks, if some tasks complete and some fail, only the latter are resubmitted.
     */
    @Test
    public void testOnlyFailedTasksAreResubmittedInMultiTaskJob() {
        JobDescriptor<BatchJobExt> twoTaskJob = changeRetryLimit(changeBatchJobSize(oneTaskBatchJobDescriptor(), 2), 1);
        jobsScenarioBuilder.scheduleJob(twoTaskJob, jobScenario -> jobScenario
                .expectJobEvent()
                .advance()
                .inActiveTasks((taskIdx, resubmit) -> ScenarioTemplates.acceptTask(taskIdx, resubmit))
                .inActiveTasks((taskIdx, resubmit) -> ScenarioTemplates.startTask(taskIdx, resubmit, TaskState.Started))
                .template(ScenarioTemplates.triggerComputeProviderFinishedEvent(0, 0, 0))
                .template(ScenarioTemplates.triggerComputeProviderFinishedEvent(1, 0, -1))
                .template(ScenarioTemplates.acceptTask(1, 1))
                .template(ScenarioTemplates.startTask(1, 1, TaskState.Started))
                .template(ScenarioTemplates.triggerComputeProviderFinishedEvent(1, 1, 0))
                .template(ScenarioTemplates.verifyJobWithFinishedTasksCompletes())
        );
    }

    /**
     * See {@link #testKillingRetryableTaskInActiveState(TaskState)}.
     */
    @Test
    public void testKillingRetryableTaskInAcceptedState() {
        testKillingRetryableTaskInActiveState(TaskState.Accepted);
    }

    /**
     * See {@link #testKillingRetryableTaskInActiveState(TaskState)}.
     */
    @Test
    public void testKillingRetryableTaskInStartInitiatedState() {
        testKillingRetryableTaskInActiveState(TaskState.Launched);
    }

    /**
     * See {@link #testKillingRetryableTaskInActiveState(TaskState)}.
     */
    @Test
    public void testKillingRetryableTaskInStartedState() {
        testKillingRetryableTaskInActiveState(TaskState.Started);
    }

    /**
     * Run a retryable job, with a task in a specific state. Check that the task is resubmitted after kill.
     */
    private void testKillingRetryableTaskInActiveState(TaskState taskState) {
        JobDescriptor<BatchJobExt> jobWithOneRetry = changeRetryLimit(oneTaskBatchJobDescriptor(), 1);
        jobsScenarioBuilder.scheduleJob(jobWithOneRetry, jobScenario -> jobScenario
                .template(ScenarioTemplates.acceptJobWithOneTask(0, 0))
                .template(ScenarioTemplates.startTask(0, 0, taskState))
                .template(ScenarioTemplates.killBatchTask(0, 0))
                .template(ScenarioTemplates.acceptTask(0, 1))
        );
    }

    @Test
    public void testKillingTaskInFenzoCallback() {
        jobsScenarioBuilder.scheduleJob(oneTaskBatchJobDescriptor(), jobScenario -> jobScenario
                .template(ScenarioTemplates.acceptJobWithOneTask(0, 0))
                .expectTaskInActiveState(0, 0, TaskState.Accepted)
                .killTask(0, 0, V3JobOperations.Trigger.Scheduler)
                .expectTaskStateChangeEvent(0, 0, TaskState.KillInitiated, TaskStatus.REASON_TRANSIENT_SYSTEM_ERROR)
                .triggerComputePlatformFinishedEvent(0, 0, -1, TaskStatus.REASON_TASK_LOST)
                .expectTaskStateChangeEvent(0, 0, TaskState.Finished, TaskStatus.REASON_TASK_LOST)
                .advance()
                .template(ScenarioTemplates.acceptTask(0, 1))
                .inTask(0, 1, task -> {
                    assertThat(task.getResubmitNumber()).isEqualTo(1);
                    assertThat(task.getSystemResubmitNumber()).isEqualTo(1);
                })
        );
    }

    /**
     * Check that killing a task that is already in KillInitiated state has no effect.
     */
    @Test
    public void testKillingTaskInKillInitiatedState() {
        jobsScenarioBuilder.scheduleJob(oneTaskBatchJobDescriptor(), jobScenario -> jobScenario
                .template(ScenarioTemplates.acceptJobWithOneTask(0, 0))
                .template(ScenarioTemplates.startTask(0, 0, TaskState.Started))
                .killTask(0, 0)
                .expectTaskUpdatedInStore(0, 0, task -> {
                    assertThat(task.getStatus().getState()).isEqualTo(TaskState.KillInitiated);
                    assertThat(task.getStatus().getReasonCode()).isEqualTo(TaskStatus.REASON_TASK_KILLED);
                })
                .expectTaskStateChangeEvent(0, 0, TaskState.KillInitiated, TaskStatus.REASON_TASK_KILLED)
                .expectFailure(() -> jobScenario.killTask(0, 0), error -> {
                    assertThat(error).isInstanceOf(JobManagerException.class);
                    assertThat(((JobManagerException) error).getErrorCode()).isEqualTo(ErrorCode.TaskTerminating);
                })
                .advance()
                .expectNoStoreUpdate()
                .expectNoTaskStateChangeEvent()
        );
    }

    /**
     * Check that killing a job with running task, terminates the task first.
     */
    @Test
    public void testKillingJobInAcceptedState() {
        jobsScenarioBuilder.scheduleJob(oneTaskBatchJobDescriptor(), jobScenario -> jobScenario
                .template(ScenarioTemplates.acceptJobWithOneTask(0, 0))
                .template(ScenarioTemplates.startTask(0, 0, TaskState.Started))
                .template(ScenarioTemplates.killJob())
                .template(ScenarioTemplates.reconcilerTaskKill(0, 0))
                .template(ScenarioTemplates.handleTaskFinishedTransitionInSingleTaskJob(0, 0, TaskStatus.REASON_TASK_KILLED))
        );
    }

    /**
     * Check killing a job with retryable tasks. There should be no task resubmit attempts.
     */
    @Test
    public void testJobKillWithTaskInAcceptedStateWithRetries() {
        jobsScenarioBuilder.scheduleJob(oneTaskBatchJobDescriptor(), jobScenario -> jobScenario
                .template(ScenarioTemplates.acceptJobWithOneTask(0, 0))
                .template(ScenarioTemplates.killJob())
                .expectTaskStateChangeEvent(0, 0, TaskState.KillInitiated)
                .advance(2 * JobsScenarioBuilder.KILL_INITIATED_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .expectTaskStateChangeEvent(0, 0, TaskState.KillInitiated)
                .triggerComputePlatformFinishedEvent(0, 0, -1, TaskStatus.REASON_TASK_LOST)
                .expectTaskStateChangeEvent(0, 0, TaskState.Finished)
                .advance().advance()
                .expectJobEvent(job -> assertThat(job.getStatus().getState() == JobState.Finished))
        );
    }

    /**
     * Check that killing a job with a failed tasks, terminates the job.
     */
    @Test
    public void testKillingJobInAcceptedStateWithFailedTasks() {
        JobDescriptor<BatchJobExt> jobWithRetries = JobFunctions.changeRetryLimit(oneTaskBatchJobDescriptor(), 2);
        jobsScenarioBuilder.scheduleJob(jobWithRetries, jobScenario -> jobScenario
                .template(ScenarioTemplates.acceptJobWithOneTask(0, 0))
                .template(ScenarioTemplates.startTask(0, 0, TaskState.Started))
                // Fail the task just before job kill operation is triggered
                .triggerComputePlatformFinishedEvent(0, 0, -1, TaskStatus.REASON_TASK_LOST)
                .template(ScenarioTemplates.killJob())
                .template(ScenarioTemplates.handleTaskFinishedTransitionInSingleTaskJob(0, 0, TaskStatus.REASON_TASK_LOST))
        );
    }

    @Test
    public void testKillingJobInKillInitiatedState() {
        jobsScenarioBuilder.scheduleJob(oneTaskBatchJobDescriptor(), jobScenario -> jobScenario
                .template(ScenarioTemplates.acceptJobWithOneTask(0, 0))
                .template(ScenarioTemplates.startTask(0, 0, TaskState.Started))
                .template(ScenarioTemplates.killJob())
                .expectTaskUpdatedInStore(0, 0, task -> assertThat(task.getStatus().getState()).isEqualTo(TaskState.KillInitiated))
                .expectTaskStateChangeEvent(0, 0, TaskState.KillInitiated)
                .expectFailure(jobScenario::killJob, error -> {
                    assertThat(error).isInstanceOf(JobManagerException.class);
                    assertThat(((JobManagerException) error).getErrorCode()).isEqualTo(ErrorCode.JobTerminating);
                })
                .expectNoStoreUpdate()
                .expectNoTaskStateChangeEvent()
        );
    }

    @Test
    public void testTaskKillConcurrencyIsLimitedWhenJobIsKilled() {
        jobsScenarioBuilder.scheduleJob(JobFunctions.changeBatchJobSize(oneTaskBatchJobDescriptor(), 100), jobScenario -> jobScenario
                .expectJobEvent()
                .advance()
                .inActiveTasks((taskIdx, resubmit) -> ScenarioTemplates.acceptTask(taskIdx, resubmit))
                .inActiveTasks((taskIdx, resubmit) -> ScenarioTemplates.startTask(taskIdx, resubmit, TaskState.Started))
                .advance()
                .allTasks(tasks -> assertThat(tasks.size() > CONCURRENT_STORE_UPDATE_LIMIT).isTrue())
                .ignoreAvailableEvents()
                .killJob()
                .expectJobEvent(job -> assertThat(job.getStatus().getState()).isEqualTo(JobState.KillInitiated))
                .advance()
                .allTasks(tasks -> {
                    long killed = tasks.stream().filter(task -> task.getStatus().getState() == TaskState.KillInitiated).count();
                    assertThat(killed).isEqualTo(CONCURRENT_STORE_UPDATE_LIMIT);
                })
        );
    }

    @Test
    public void testSystemErrorsAreRetriedAlways() {
        JobDescriptor<BatchJobExt> jobWithRetries = JobFunctions.changeRetryPolicy(
                oneTaskBatchJobDescriptor(),
                JobModel.newImmediateRetryPolicy().withRetries(0).build()
        );
        jobsScenarioBuilder.scheduleJob(jobWithRetries, jobScenario -> jobScenario
                .template(ScenarioTemplates.acceptJobWithOneTask(0, 0))
                .template(ScenarioTemplates.startTask(0, 0, TaskState.Started))
                // Fail the task just before job kill operation is triggered
                .triggerComputePlatformFinishedEvent(0, 0, -1, TaskStatus.REASON_LOCAL_SYSTEM_ERROR)
                .advance(60, TimeUnit.SECONDS)
                .expectTaskStateChangeEvent(0, 1, TaskState.Accepted)
        );
    }

    @Test
    public void testStuckInLaunchedIsRetriedAlways() {
        JobDescriptor<BatchJobExt> jobWithRetries = JobFunctions.changeRetryPolicy(
                oneTaskBatchJobDescriptor(),
                JobModel.newImmediateRetryPolicy().withRetries(0).build()
        );
        jobsScenarioBuilder.scheduleJob(jobWithRetries, jobScenario -> jobScenario
                .template(ScenarioTemplates.acceptJobWithOneTask(0, 0))
                .template(ScenarioTemplates.startTask(0, 0, TaskState.StartInitiated))
                // Task will time out and move to KillInitiated by the system
                .advance(120, TimeUnit.SECONDS)
                .advance(JobsScenarioBuilder.MIN_RETRY_INTERVAL_MS, TimeUnit.MILLISECONDS)
                .expectTaskStateChangeEvent(0, 1, TaskState.Accepted)
        );
    }

    /**
     * Check task timeout in Launched state. if the timeout passes, task should be moved to KillInitiated state.
     */
    @Test
    public void testTaskLaunchingTimeout() {
        jobsScenarioBuilder.scheduleJob(oneTaskBatchJobDescriptor(), jobScenario -> jobScenario
                .template(ScenarioTemplates.acceptJobWithOneTask(0, 0))
                .template(ScenarioTemplates.startTask(0, 0, TaskState.Launched))
                .advance()
                .advance(JobsScenarioBuilder.LAUNCHED_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .advance()
                .expectTaskInActiveState(0, 0, TaskState.KillInitiated)
        );
    }

    /**
     * Check task timeout in StartInitiated state. if the timeout passes, task should be moved to KillInitiated state.
     */
    @Test
    public void testStartInitiatedTimeout() {
        jobsScenarioBuilder.scheduleJob(oneTaskBatchJobDescriptor(), jobScenario -> jobScenario
                .template(ScenarioTemplates.acceptJobWithOneTask(0, 0))
                .template(ScenarioTemplates.startTask(0, 0, TaskState.StartInitiated))
                .advance()
                .advance(JobsScenarioBuilder.START_INITIATED_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .advance()
                .expectTaskInActiveState(0, 0, TaskState.KillInitiated)
        );
    }

    /**
     * If timeout passes in KillInitiated state, instead of moving directly to Finished state, check that configured
     * number of kill reattempts is made. The total timeout in this state is (attempts_count * timeout).
     */
    @Test
    public void testKillReattemptsInKillInitiatedTimeout() {
        jobsScenarioBuilder.scheduleJob(oneTaskBatchJobDescriptor(), jobScenario -> jobScenario
                .template(ScenarioTemplates.acceptJobWithOneTask(0, 0))
                .template(ScenarioTemplates.startTask(0, 0, TaskState.StartInitiated))
                .template(ScenarioTemplates.killTask(0, 0))
                .template(ScenarioTemplates.passKillInitiatedTimeoutWithKillReattempt(0, 0))
                .template(ScenarioTemplates.passFinalKillInitiatedTimeout())
                .template(ScenarioTemplates.handleTaskFinishedTransitionInSingleTaskJob(0, 0, TaskStatus.REASON_STUCK_IN_KILLING_STATE))
        );
    }

    @Test
    public void testJobCapacityUpdateNotPossibleForBatchJob() throws Exception {
        jobsScenarioBuilder.scheduleJob(oneTaskBatchJobDescriptor(), jobScenario -> jobScenario
                .expectFailure(() -> jobScenario.changeCapacity(0, 5, 10), error -> {
                    assertThat(error).isInstanceOf(JobManagerException.class);
                    assertThat(((JobManagerException) error).getErrorCode()).isEqualTo(ErrorCode.NotServiceJob);
                })
        );
    }

    @Test
    public void testJobEnableStatusNotPossibleForBatchJob() {
        jobsScenarioBuilder.scheduleJob(oneTaskBatchJobDescriptor(), jobScenario -> jobScenario
                .expectFailure(() -> jobScenario.changeJobEnabledStatus(false), error -> {
                    assertThat(error).isInstanceOf(JobManagerException.class);
                    assertThat(((JobManagerException) error).getErrorCode()).isEqualTo(ErrorCode.NotServiceJob);
                })
        );
    }

    @Test
    public void testBatchJobRuntimeLimitWithRetries() {
        testBatchJobRuntimeLimit(true);
    }

    @Test
    public void testBatchJobRuntimeLimitWithNoRetries() {
        testBatchJobRuntimeLimit(false);
    }

    @Test
    public void testBatchJobRetriesWhenTaskCreateWriteToStoreFails() {
        jobsScenarioBuilder.breakStoreForTasks()
                .scheduleJob(oneTaskBatchJobDescriptor(), jobScenario -> jobScenario
                        .advance()
                        .enableStore()
                        .advance(500, TimeUnit.MILLISECONDS)
                        .expectTaskEvent(0, 0, event -> {
                            Task task = event.getCurrentTask();
                            assertThat(task.getOriginalId()).isEqualTo(task.getId());
                        })
                );
    }

    private void testBatchJobRuntimeLimit(boolean retryOnRuntimeLimit) {
        JobDescriptor<BatchJobExt> jobWithRuntimeLimit = oneTaskBatchJobDescriptor().but(jd ->
                jd.getExtensions().toBuilder()
                        .withRetryPolicy(JobModel.newImmediateRetryPolicy().withRetries(1).build())
                        .withRuntimeLimitMs(120_000)
                        .withRetryOnRuntimeLimit(retryOnRuntimeLimit)
                        .build()
        );
        jobsScenarioBuilder.scheduleJob(jobWithRuntimeLimit, jobScenario -> jobScenario
                .template(ScenarioTemplates.acceptJobWithOneTask(0, 0))
                .template(ScenarioTemplates.startTask(0, 0, TaskState.Started))
                .advance(120_000, TimeUnit.MILLISECONDS)
                .expectTaskStateChangeEvent(0, 0, TaskState.KillInitiated, TaskStatus.REASON_RUNTIME_LIMIT_EXCEEDED)
                .triggerComputePlatformFinishedEvent(0, 0, -1, TaskStatus.REASON_TASK_KILLED)
                .andThen(() -> {
                            if (retryOnRuntimeLimit) {
                                jobScenario.expectTaskAddedToStore(0, 1, task -> assertThat(task.getResubmitNumber()).isEqualTo(1));
                            } else {
                                jobScenario.expectJobEvent(job -> assertThat(job.getStatus().getState()).isEqualTo(JobState.Finished));
                            }
                        }
                )
        );
    }
}
