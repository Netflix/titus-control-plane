package io.netflix.titus.master.jobmanager.service.integration;

import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.api.jobmanager.service.JobManagerException;
import io.netflix.titus.master.jobmanager.service.integration.scenario.JobsScenarioBuilder;
import io.netflix.titus.master.jobmanager.service.integration.scenario.ScenarioTemplates;
import io.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JobSchedulingCommonTest {

    private final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder();

    @Test
    public void testBatchJobWithTaskInAcceptedStateNotScheduledYet() {
        testJobWithTaskInAcceptedStateNotScheduledYet(JobDescriptorGenerator.oneTaskBatchJobDescriptor());
    }

    @Test
    public void testServiceJobWithTaskInAcceptedStateNotScheduledYet() {
        testJobWithTaskInAcceptedStateNotScheduledYet(JobDescriptorGenerator.oneTaskServiceJobDescriptor());
    }

    /**
     * This test covers the case where a task is created in store, but not added to Fenzo yet, and the job while in this
     * state is terminated.
     */
    private void testJobWithTaskInAcceptedStateNotScheduledYet(JobDescriptor<?> oneTaskJobDescriptor) {
        jobsScenarioBuilder.scheduleJob(oneTaskJobDescriptor, jobScenario -> jobScenario
                .expectJobEvent()
                .expectTaskAddedToStore(0, 0, task -> assertThat(task.getStatus().getState()).isEqualTo(TaskState.Accepted))
                .template(ScenarioTemplates.killJob())
                .expectTaskStateChangeEvent(0, 0, TaskState.Accepted)
                .template(ScenarioTemplates.handleTaskFinishedTransitionInSingleTaskJob(0, 0, TaskStatus.REASON_TASK_KILLED))
        );
    }

    @Test
    public void testTryToLaunchTaskWhichIsInFinishedState() {
        jobsScenarioBuilder.scheduleJob(JobDescriptorGenerator.oneTaskServiceJobDescriptor(), jobScenario -> jobScenario
                .expectJobEvent()
                .expectTaskStateChangeEvent(0, 0, TaskState.Accepted)
                .expectScheduleRequest(0, 0)
                .killTask(0, 0)
                .expectTaskStateChangeEvent(0, 0, TaskState.KillInitiated)
                .triggerMesosFinishedEvent(0, 0, -1, TaskStatus.REASON_TASK_LOST)
                .triggerFailingSchedulerLaunchEvent(0, 0, error -> assertThat(error).isInstanceOf(JobManagerException.class))
                .advance() // Advance to observe 'status=error type=afterChange' in the log file
        );
    }
}
