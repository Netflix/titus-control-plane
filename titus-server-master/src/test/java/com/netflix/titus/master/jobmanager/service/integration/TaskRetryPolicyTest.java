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
import com.netflix.titus.api.jobmanager.model.job.JobModel;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.model.job.retry.DelayedRetryPolicy;
import com.netflix.titus.api.jobmanager.model.job.retry.ExponentialBackoffRetryPolicy;
import com.netflix.titus.api.jobmanager.model.job.retry.ImmediateRetryPolicy;
import com.netflix.titus.master.jobmanager.service.integration.scenario.JobScenarioBuilder;
import com.netflix.titus.master.jobmanager.service.integration.scenario.JobsScenarioBuilder;
import com.netflix.titus.master.jobmanager.service.integration.scenario.ScenarioTemplates;
import org.junit.Before;
import org.junit.Test;

import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.changeRetryPolicy;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskServiceJobDescriptor;

public class TaskRetryPolicyTest {

    private static final ImmediateRetryPolicy NO_RETRIES = JobModel.newImmediateRetryPolicy().withRetries(0).build();

    private static final ImmediateRetryPolicy IMMEDIATE = JobModel.newImmediateRetryPolicy().withRetries(5).build();
    private static final int[] IMMEDIATE_DELAYS_SEC = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

    private static final DelayedRetryPolicy DELAYED = JobModel.newDelayedRetryPolicy().withDelay(5, TimeUnit.SECONDS).withRetries(5).build();
    private static final int[] DELAYED_POLICY_DELAYS_SEC = {5, 5, 5, 5, 5, 5, 5, 5, 5, 5};

    private static final ExponentialBackoffRetryPolicy EXPONENTIAL = JobModel.newExponentialBackoffRetryPolicy()
            .withInitialDelayMs(1_000)
            .withMaxDelayMs(10_000)
            .withRetries(5)
            .build();
    private static final int[] EXPONENTIAL_DELAYS_SEC = {1, 2, 4, 8, 10, 10, 10, 10, 10, 10};

    private JobsScenarioBuilder jobsScenarioBuilder;

    @Before
    public void setUp() throws Exception {
        this.jobsScenarioBuilder = new JobsScenarioBuilder();
    }

    @Test
    public void testBatchImmediateRetry() throws Exception {
        JobDescriptor<BatchJobExt> jobWithRetries = changeRetryPolicy(oneTaskBatchJobDescriptor(), IMMEDIATE);
        batchRunAndFail(jobWithRetries, IMMEDIATE_DELAYS_SEC, TimeUnit.SECONDS);
    }

    @Test
    public void testServiceImmediateRetry() throws Exception {
        JobDescriptor<ServiceJobExt> jobWithRetries = changeRetryPolicy(oneTaskServiceJobDescriptor(), IMMEDIATE);
        serviceRunAndFail(jobWithRetries, IMMEDIATE_DELAYS_SEC, TimeUnit.SECONDS);
    }

    @Test
    public void testBatchDelayedRetry() throws Exception {
        JobDescriptor<BatchJobExt> jobWithRetries = changeRetryPolicy(oneTaskBatchJobDescriptor(), DELAYED);
        batchRunAndFail(jobWithRetries, DELAYED_POLICY_DELAYS_SEC, TimeUnit.SECONDS);
    }

    @Test
    public void testServiceDelayedRetry() throws Exception {
        JobDescriptor<ServiceJobExt> jobWithRetries = changeRetryPolicy(oneTaskServiceJobDescriptor(), DELAYED);
        serviceRunAndFail(jobWithRetries, DELAYED_POLICY_DELAYS_SEC, TimeUnit.SECONDS);
    }

    @Test
    public void testBatchExponentialBackoffRetry() throws Exception {
        JobDescriptor<BatchJobExt> jobWithRetries = changeRetryPolicy(oneTaskBatchJobDescriptor(), EXPONENTIAL);
        batchRunAndFail(jobWithRetries, EXPONENTIAL_DELAYS_SEC, TimeUnit.SECONDS);
    }

    @Test
    public void testServiceExponentialBackoffRetry() throws Exception {
        JobDescriptor<ServiceJobExt> jobWithRetries = changeRetryPolicy(oneTaskServiceJobDescriptor(), EXPONENTIAL);
        serviceRunAndFail(jobWithRetries, EXPONENTIAL_DELAYS_SEC, TimeUnit.SECONDS);
    }

    private void batchRunAndFail(JobDescriptor<BatchJobExt> jobWithRetries, int[] delays, TimeUnit timeUnit) {
        int retryLimit = jobWithRetries.getExtensions().getRetryPolicy().getRetries();
        JobScenarioBuilder jobScenario = runJob(jobWithRetries);
        failRetryableTask(delays, timeUnit, retryLimit);
        jobScenario.advance().template(ScenarioTemplates.failLastBatchRetryableTask(0, retryLimit));
    }

    private void serviceRunAndFail(JobDescriptor<ServiceJobExt> jobWithRetries, int[] delays, TimeUnit timeUnit) {
        int retryLimit = jobWithRetries.getExtensions().getRetryPolicy().getRetries();
        runJob(jobWithRetries);
        failRetryableTask(delays, timeUnit, retryLimit * 2); // Service jobs ignore retry limit value
    }

    @Test
    public void testBatchRetryPolicyResetIfTaskInStartedStateLongEnough() throws Exception {
        JobDescriptor<BatchJobExt> jobWithRetries = changeRetryPolicy(
                oneTaskBatchJobDescriptor().but(jd ->
                        jd.getExtensions().toBuilder().withRuntimeLimitMs(3600_000).build() // Prevent runtimeLimit timeout
                ),
                EXPONENTIAL
        );
        int retryLimit = jobWithRetries.getExtensions().getRetryPolicy().getRetries();
        testRetryPolicyResetIfTaskInStartedStateLongEnough(jobWithRetries, retryLimit);
    }

    @Test
    public void testServiceRetryPolicyResetIfTaskInStartedStateLongEnough() throws Exception {
        JobDescriptor<ServiceJobExt> jobWithRetries = changeRetryPolicy(oneTaskServiceJobDescriptor(), EXPONENTIAL);
        int retryLimit = jobWithRetries.getExtensions().getRetryPolicy().getRetries();
        testRetryPolicyResetIfTaskInStartedStateLongEnough(jobWithRetries, retryLimit);
    }

    @Test
    public void testBatchSystemRetry() {
        JobDescriptor<BatchJobExt> jobWithoutRetries = changeRetryPolicy(oneTaskBatchJobDescriptor(), NO_RETRIES);
        runJob(jobWithoutRetries)
                .triggerComputePlatformFinishedEvent(0, 0, -1, TaskStatus.REASON_LOCAL_SYSTEM_ERROR)
                .template(ScenarioTemplates.cleanAfterFinishedTaskAndRetry(0, 0, TaskStatus.REASON_LOCAL_SYSTEM_ERROR, 0L));
    }

    private void testRetryPolicyResetIfTaskInStartedStateLongEnough(JobDescriptor<?> jobWithRetries, int retryLimit) {
        JobScenarioBuilder jobScenario = runJob(jobWithRetries);
        failRetryableTask(EXPONENTIAL_DELAYS_SEC, TimeUnit.SECONDS, retryLimit - 1);

        // Start the active task, and keep it running long enough to reset retryer
        jobScenario
                .template(ScenarioTemplates.startTask(0, retryLimit - 1, TaskState.Started))
                .advance(5, TimeUnit.MINUTES);

        // Now fail the task again, and expect it to restart immediately.
        jobScenario.template(ScenarioTemplates.failRetryableTask(0, retryLimit - 1, 0));
    }

    private JobScenarioBuilder runJob(JobDescriptor<?> job) {
        jobsScenarioBuilder.scheduleJob(job, jobScenario -> jobScenario
                .expectJobEvent()
                .template(ScenarioTemplates.acceptTask(0, 0))
        );
        return jobsScenarioBuilder.getJobScenario(0);
    }

    private void failRetryableTask(int[] delays, TimeUnit timeUnit, int retries) {
        JobScenarioBuilder jobScenario = jobsScenarioBuilder.getJobScenario(0);
        for (int i = 0; i < retries; i++) {
            int retryDelay = delays[i];
            jobScenario.template(ScenarioTemplates.failRetryableTask(0, i, timeUnit.toMillis(retryDelay)));
        }
    }
}
