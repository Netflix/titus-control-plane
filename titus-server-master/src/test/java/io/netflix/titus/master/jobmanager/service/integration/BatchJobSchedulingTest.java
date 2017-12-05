/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.jobmanager.service.integration;

import java.util.concurrent.TimeUnit;

import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import io.netflix.titus.master.jobmanager.service.integration.scenario.JobsScenarioBuilder;
import io.netflix.titus.master.jobmanager.service.integration.scenario.ScenarioTemplates;
import org.junit.Test;

import static io.netflix.titus.api.jobmanager.model.job.JobFunctions.changeRetryLimit;
import static io.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;
import static org.assertj.core.api.Assertions.assertThat;

public class BatchJobSchedulingTest {

    private final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder();

    /**
     * Run single task batch job that terminates with exit code 0.
     */
    @Test
    public void testContainerFinishOkAndNoRetry() throws Exception {
        jobsScenarioBuilder.scheduleBatchJob(oneTaskBatchJobDescriptor(), jobScenario -> jobScenario
                .template(ScenarioTemplates.acceptJobWithOneTask(0, 0))
                .template(ScenarioTemplates.startTask(0, 0, TaskState.Started))
                .template(ScenarioTemplates.finishSingleTaskJob(0, 0, TaskStatus.REASON_NORMAL, 0))
        );
    }

    /**
     * Check that containers terminating with exit code 0 are not restarted.
     */
    @Test
    public void testContainerFinishOkAndOneRetry() throws Exception {
        JobDescriptor<BatchJobExt> jobWithOneRetry = changeRetryLimit(oneTaskBatchJobDescriptor(), 1);
        jobsScenarioBuilder.scheduleBatchJob(jobWithOneRetry, jobScenario -> jobScenario
                .template(ScenarioTemplates.acceptJobWithOneTask(0, 0))
                .template(ScenarioTemplates.startTask(0, 0, TaskState.Started))
                .template(ScenarioTemplates.finishSingleTaskJob(0, 0, TaskStatus.REASON_NORMAL, 0))
        );
    }

    /**
     * Check that containers that fail with non zero exit code, and are not retryable, are not restarted.
     */
    @Test
    public void testContainerFinishWitNonZeroErrorCodeAndNoRetry() throws Exception {
        jobsScenarioBuilder.scheduleBatchJob(oneTaskBatchJobDescriptor(), jobScenario -> jobScenario
                .template(ScenarioTemplates.acceptJobWithOneTask(0, 0))
                .template(ScenarioTemplates.startTask(0, 0, TaskState.Started))
                .template(ScenarioTemplates.finishSingleTaskJob(0, 0, TaskStatus.REASON_FAILED, -1))
        );
    }

    /**
     * Check container restart for failing containers that are retryable.
     */
    @Test
    public void testContainerFinishWitNonZeroErrorCodeAndOneRetry() throws Exception {
        JobDescriptor<BatchJobExt> jobWithOneRetry = changeRetryLimit(oneTaskBatchJobDescriptor(), 1);
        jobsScenarioBuilder.scheduleBatchJob(jobWithOneRetry, jobScenario -> jobScenario
                .template(ScenarioTemplates.acceptJobWithOneTask(0, 0))
                .template(ScenarioTemplates.startTask(0, 0, TaskState.Started))
                .template(ScenarioTemplates.failRetryableTask(0, 0))
                .template(ScenarioTemplates.failLastRetryableTask(0, 1))
        );
    }

    /**
     * See {@link #testKillingRetryableTaskInActiveState(TaskState)}.
     */
    @Test
    public void testKillingRetryableTaskInAcceptedState() throws Exception {
        testKillingRetryableTaskInActiveState(TaskState.Accepted);
    }

    /**
     * See {@link #testKillingRetryableTaskInActiveState(TaskState)}.
     */
    @Test
    public void testKillingRetryableTaskInStartInitiatedState() throws Exception {
        testKillingRetryableTaskInActiveState(TaskState.Launched);
    }

    /**
     * See {@link #testKillingRetryableTaskInActiveState(TaskState)}.
     */
    @Test
    public void testKillingRetryableTaskInStartedState() throws Exception {
        testKillingRetryableTaskInActiveState(TaskState.Started);
    }

    /**
     * Run a retryable job, with a task in a specific state. Check that the task is resubmitted after kill.
     */
    private void testKillingRetryableTaskInActiveState(TaskState taskState) throws Exception {
        JobDescriptor<BatchJobExt> jobWithOneRetry = changeRetryLimit(oneTaskBatchJobDescriptor(), 1);
        jobsScenarioBuilder.scheduleBatchJob(jobWithOneRetry, jobScenario -> jobScenario
                .template(ScenarioTemplates.acceptJobWithOneTask(0, 0))
                .template(ScenarioTemplates.startTask(0, 0, taskState))
                .template(ScenarioTemplates.killRetryableTask(0, 0))
        );
    }

    /**
     * Check that killing a task that is already in KillInitiated state has no effect.
     */
    @Test
    public void testKillingTaskInKillInitiatedState() throws Exception {
        jobsScenarioBuilder.scheduleBatchJob(oneTaskBatchJobDescriptor(), jobScenario -> jobScenario
                .template(ScenarioTemplates.acceptJobWithOneTask(0, 0))
                .template(ScenarioTemplates.startTask(0, 0, TaskState.Started))
                .killTask(0, 0)
                .expectTaskUpdatedInStore(0, 0, task -> {
                    assertThat(task.getStatus().getState()).isEqualTo(TaskState.KillInitiated);
                    assertThat(task.getStatus().getReasonCode()).isEqualTo(TaskStatus.REASON_TASK_KILLED);
                })
                .expectBatchTaskStateChangeEvent(0, 0, TaskState.KillInitiated, TaskStatus.REASON_TASK_KILLED)
                .killTask(0, 0)
                .advance()
                .expectNoStoreUpdate()
                .expectNoTaskStateChangeEvent()
        );
    }

    /**
     * Check task timeout in Launched state. if the timeout passes, task should be moved to KillInitiated state.
     */
    @Test
    public void testTaskLaunchingTimeout() throws Exception {
        jobsScenarioBuilder.scheduleBatchJob(oneTaskBatchJobDescriptor(), jobScenario -> jobScenario
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
    public void testStartInitiatedTimeout() throws Exception {
        jobsScenarioBuilder.scheduleBatchJob(oneTaskBatchJobDescriptor(), jobScenario -> jobScenario
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
     * number of Mesos kill reattempts is made. The total timeout in this state is (attempts_count * timeout).
     */
    @Test
    public void testKillReattemptsInKillInitiatedTimeout() throws Exception {
        jobsScenarioBuilder.scheduleBatchJob(oneTaskBatchJobDescriptor(), jobScenario -> jobScenario
                .template(ScenarioTemplates.acceptJobWithOneTask(0, 0))
                .template(ScenarioTemplates.startTask(0, 0, TaskState.StartInitiated))
                .template(ScenarioTemplates.killTask(0, 0))
                .template(ScenarioTemplates.passKillInitiatedTimeoutWithKillReattempt(0, 0))
                .template(ScenarioTemplates.passFinalKillInitiatedTimeout())
                .template(ScenarioTemplates.handleTaskFinishedTransitionInSingleTaskJob(0, 0, TaskStatus.REASON_STUCK_IN_STATE))
        );
    }
}
