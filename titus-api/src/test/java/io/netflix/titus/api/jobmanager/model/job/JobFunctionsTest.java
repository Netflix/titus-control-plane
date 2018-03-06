package io.netflix.titus.api.jobmanager.model.job;

import io.netflix.titus.common.util.time.Clock;
import io.netflix.titus.common.util.time.Clocks;
import io.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import io.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JobFunctionsTest {

    private static final Task REFERENCE_TASK = JobGenerator.batchTasks(JobGenerator.batchJobs(JobDescriptorGenerator.oneTaskBatchJobDescriptor()).getValue()).getValue();

    private final Clock clock = Clocks.system();

    @Test
    public void testGetTimeInStateForCompletedTask() {
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
        assertThat(JobFunctions.getTimeInState(task, TaskState.Accepted, clock)).contains(100L);
        assertThat(JobFunctions.getTimeInState(task, TaskState.Launched, clock)).contains(100L);
        assertThat(JobFunctions.getTimeInState(task, TaskState.StartInitiated, clock)).contains(800L);
        assertThat(JobFunctions.getTimeInState(task, TaskState.Started, clock)).contains(1000L);
        assertThat(JobFunctions.getTimeInState(task, TaskState.KillInitiated, clock)).contains(3000L);
        assertThat(JobFunctions.getTimeInState(task, TaskState.Finished, clock).get()).isGreaterThan(0);
    }

    @Test
    public void testGetTimeInStateForFailedTask() {
        TaskStatus checked = TaskStatus.newBuilder().withState(TaskState.KillInitiated).withTimestamp(1000).build();
        Task task = REFERENCE_TASK.toBuilder()
                .withStatus(checked)
                .withStatusHistory(
                        TaskStatus.newBuilder().withState(TaskState.Accepted).withTimestamp(0).build(),
                        TaskStatus.newBuilder().withState(TaskState.Launched).withTimestamp(100).build()
                )
                .build();
        assertThat(JobFunctions.getTimeInState(task, TaskState.Accepted, clock)).contains(100L);
        assertThat(JobFunctions.getTimeInState(task, TaskState.Launched, clock)).contains(900L);
        assertThat(JobFunctions.getTimeInState(task, TaskState.StartInitiated, clock)).isEmpty();
        assertThat(JobFunctions.getTimeInState(task, TaskState.Started, clock)).isEmpty();
        assertThat(JobFunctions.getTimeInState(task, TaskState.KillInitiated, clock).get()).isGreaterThan(0);
        assertThat(JobFunctions.getTimeInState(task, TaskState.Finished, clock)).isEmpty();
    }

    @Test
    public void testHasTransition() {
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
        assertThat(JobFunctions.containsExactlyTaskStates(task, TaskState.Accepted, TaskState.Launched, TaskState.StartInitiated, TaskState.KillInitiated)).isTrue();
    }
}