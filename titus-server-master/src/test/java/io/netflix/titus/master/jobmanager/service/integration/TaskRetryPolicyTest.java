package io.netflix.titus.master.jobmanager.service.integration;

import java.util.concurrent.TimeUnit;

import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor.JobDescriptorExt;
import io.netflix.titus.api.jobmanager.model.job.JobModel;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.api.jobmanager.model.job.retry.DelayedRetryPolicy;
import io.netflix.titus.api.jobmanager.model.job.retry.ExponentialBackoffRetryPolicy;
import io.netflix.titus.api.jobmanager.model.job.retry.ImmediateRetryPolicy;
import io.netflix.titus.master.jobmanager.service.integration.scenario.JobScenarioBuilder;
import io.netflix.titus.master.jobmanager.service.integration.scenario.JobsScenarioBuilder;
import io.netflix.titus.master.jobmanager.service.integration.scenario.ScenarioTemplates;
import org.junit.Test;

import static io.netflix.titus.api.jobmanager.model.job.JobFunctions.changeRetryPolicy;
import static io.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;
import static io.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskServiceJobDescriptor;

public class TaskRetryPolicyTest {

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

    private final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder();

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
        JobScenarioBuilder<BatchJobExt> jobScenario = runJob(jobWithRetries);
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

    private void testRetryPolicyResetIfTaskInStartedStateLongEnough(JobDescriptor<?> jobWithRetries, int retryLimit) {
        JobScenarioBuilder<?> jobScenario = runJob(jobWithRetries);
        failRetryableTask(EXPONENTIAL_DELAYS_SEC, TimeUnit.SECONDS, retryLimit - 1);

        // Start the active task, and keep it running long enough to reset retryer
        jobScenario
                .template(ScenarioTemplates.startTask(0, retryLimit - 1, TaskState.Started))
                .advance(5, TimeUnit.MINUTES);

        // Now fail the task again, and expect it to restart immediately.
        jobScenario.template(ScenarioTemplates.failRetryableTask(0, retryLimit - 1, 0));
    }

    private <E extends JobDescriptorExt> JobScenarioBuilder<E> runJob(JobDescriptor<E> job) {
        jobsScenarioBuilder.scheduleJob(job, jobScenario -> jobScenario
                .expectJobEvent()
                .template(ScenarioTemplates.acceptTask(0, 0))
        );
        return jobsScenarioBuilder.getJobScenario(0);
    }

    private void failRetryableTask(int[] delays, TimeUnit timeUnit, int retries) {
        JobScenarioBuilder<BatchJobExt> jobScenario = jobsScenarioBuilder.getJobScenario(0);
        for (int i = 0; i < retries; i++) {
            int retryDelay = delays[i];
            jobScenario.template(ScenarioTemplates.failRetryableTask(0, i, timeUnit.toMillis(retryDelay)));
        }
    }
}
