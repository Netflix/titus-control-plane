package io.netflix.titus.master.jobmanager.service.integration;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor.JobDescriptorExt;
import io.netflix.titus.api.jobmanager.model.job.JobModel;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.master.jobmanager.service.integration.scenario.JobScenarioBuilder;
import io.netflix.titus.master.jobmanager.service.integration.scenario.JobsScenarioBuilder;
import io.netflix.titus.master.jobmanager.service.integration.scenario.ScenarioTemplates;
import org.junit.Test;

import static io.netflix.titus.api.jobmanager.model.job.JobFunctions.changeBatchJobSize;
import static io.netflix.titus.api.jobmanager.model.job.JobFunctions.changeRetryPolicy;
import static io.netflix.titus.api.jobmanager.model.job.JobFunctions.changeServiceJobCapacity;
import static io.netflix.titus.master.jobmanager.service.integration.scenario.JobsScenarioBuilder.ACTIVE_NOT_STARTED_TASKS_LIMIT;
import static io.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;
import static io.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskServiceJobDescriptor;
import static org.assertj.core.api.Assertions.assertThat;

public class JobRateLimitingTest {

    private static final long RETREY_DELAY_MS = 1_000;

    private static final JobDescriptor<BatchJobExt> LARGE_BATCH_JOB = changeRetryPolicy(
            changeBatchJobSize(oneTaskBatchJobDescriptor(), 3 * ACTIVE_NOT_STARTED_TASKS_LIMIT),
            JobModel.newDelayedRetryPolicy().withDelay(RETREY_DELAY_MS, TimeUnit.MILLISECONDS).withRetries(1).build()
    );

    private static final JobDescriptor<ServiceJobExt> LARGE_SERVIE_JOB = changeRetryPolicy(
            changeServiceJobCapacity(oneTaskServiceJobDescriptor(), 3 * ACTIVE_NOT_STARTED_TASKS_LIMIT),
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
        jobScenario.inAllTasks(filterActiveTasks(tasks, ACTIVE_NOT_STARTED_TASKS_LIMIT), (taskIdx, resubmit) ->
                jobScenario
                        .template(ScenarioTemplates.acceptTask(taskIdx, resubmit))
                        .template(ScenarioTemplates.startTask(taskIdx, resubmit, TaskState.Started))
        );
    }

    private <E extends JobDescriptorExt> void acceptAndFailNewPartition(JobScenarioBuilder<E> jobScenario, List<Task> tasks) {
        jobScenario.inAllTasks(filterActiveTasks(tasks, ACTIVE_NOT_STARTED_TASKS_LIMIT), (taskIdx, resubmit) ->
                jobScenario
                        .template(ScenarioTemplates.acceptTask(taskIdx, resubmit))
                        .template(ScenarioTemplates.failRetryableTask(taskIdx, resubmit, RETREY_DELAY_MS))
        );
    }

    private <E extends JobDescriptorExt> void startNewBatch(JobScenarioBuilder<E> jobScenario, List<Task> tasks) {
        jobScenario.inAllTasks(filterActiveTasks(tasks, ACTIVE_NOT_STARTED_TASKS_LIMIT), (taskIdx, resubmit) ->
                jobScenario
                        .template(ScenarioTemplates.startTask(taskIdx, resubmit, TaskState.Started))
        );
    }

    private void expectAllStarted(List<Task> tasks) {
        assertThat(tasks).hasSize(3 * ACTIVE_NOT_STARTED_TASKS_LIMIT);
        assertThat(tasks.stream().filter(t -> t.getStatus().getState() == TaskState.Started)).hasSize(3 * ACTIVE_NOT_STARTED_TASKS_LIMIT);
    }

    private List<Task> filterActiveTasks(List<Task> tasks, int expected) {
        List<Task> active = tasks.stream().filter(t -> t.getStatus().getState() == TaskState.Accepted).collect(Collectors.toList());
        assertThat(active).describedAs("Expected to find %d active tasks, but is %d", expected, active.size()).hasSize(expected);
        return active;
    }
}
