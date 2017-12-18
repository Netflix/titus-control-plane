package io.netflix.titus.api.jobmanager.model.job;

import io.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import io.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JobFunctionsTest {

    private static final Task REFERENCE_TASK = JobGenerator.batchTasks(JobGenerator.batchJobs(JobDescriptorGenerator.oneTaskBatchJobDescriptor()).getValue()).getValue();

    @Test
    public void testGetTimeInStateForCompletedTask() throws Exception {
        TaskStatus checked = TaskStatus.newBuilder().withState(TaskState.Finished).withTimestamp(5000).build();
        Task task = REFERENCE_TASK.toBuilder()
                .withStatus(checked)
                .withStatusHistory(
                        TaskStatus.newBuilder().withState(TaskState.Accepted).withTimestamp(0).build(),
                        TaskStatus.newBuilder().withState(TaskState.Launched).withTimestamp(100).build(),
                        TaskStatus.newBuilder().withState(TaskState.StartInitiated).withTimestamp(200).build(),
                        TaskStatus.newBuilder().withState(TaskState.Started).withTimestamp(1000).build(),
                        TaskStatus.newBuilder().withState(TaskState.KillInitiated).withTimestamp(2000).build()
                )
                .build();
        assertThat(JobFunctions.getTimeInState(task, TaskState.Accepted)).contains(100L);
        assertThat(JobFunctions.getTimeInState(task, TaskState.Launched)).contains(100L);
        assertThat(JobFunctions.getTimeInState(task, TaskState.StartInitiated)).contains(800L);
        assertThat(JobFunctions.getTimeInState(task, TaskState.Started)).contains(1000L);
        assertThat(JobFunctions.getTimeInState(task, TaskState.KillInitiated)).contains(3000L);
        assertThat(JobFunctions.getTimeInState(task, TaskState.Finished).get()).isGreaterThan(0);
    }

    @Test
    public void testGetTimeInStateForFailedTask() throws Exception {
        TaskStatus checked = TaskStatus.newBuilder().withState(TaskState.KillInitiated).withTimestamp(1000).build();
        Task task = REFERENCE_TASK.toBuilder()
                .withStatus(checked)
                .withStatusHistory(
                        TaskStatus.newBuilder().withState(TaskState.Accepted).withTimestamp(0).build(),
                        TaskStatus.newBuilder().withState(TaskState.Launched).withTimestamp(100).build()
                )
                .build();
        assertThat(JobFunctions.getTimeInState(task, TaskState.Accepted)).contains(100L);
        assertThat(JobFunctions.getTimeInState(task, TaskState.Launched)).contains(900L);
        assertThat(JobFunctions.getTimeInState(task, TaskState.StartInitiated)).isEmpty();
        assertThat(JobFunctions.getTimeInState(task, TaskState.Started)).isEmpty();
        assertThat(JobFunctions.getTimeInState(task, TaskState.KillInitiated).get()).isGreaterThan(0);
        assertThat(JobFunctions.getTimeInState(task, TaskState.Finished)).isEmpty();
    }

    @Test
    public void testHasTransition() throws Exception {
        TaskStatus checked = TaskStatus.newBuilder().withState(TaskState.KillInitiated).withTimestamp(1000).build();
        Task task = REFERENCE_TASK.toBuilder()
                .withStatus(checked)
                .withStatusHistory(
                        TaskStatus.newBuilder().withState(TaskState.Accepted).withTimestamp(0).build(),
                        TaskStatus.newBuilder().withState(TaskState.Launched).withTimestamp(100).build(),
                        TaskStatus.newBuilder().withState(TaskState.StartInitiated).withReasonCode("step1").withTimestamp(100).build(),
                        TaskStatus.newBuilder().withState(TaskState.StartInitiated).withReasonCode("step2").withTimestamp(100).build()
                )
                .build();
        assertThat(JobFunctions.hasTransition(task, TaskState.Accepted, TaskState.Launched, TaskState.StartInitiated, TaskState.KillInitiated)).isTrue();
    }
}