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

package com.netflix.titus.master.kubernetes;

import java.time.Duration;
import java.util.Optional;

import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.common.util.time.TestClock;
import com.netflix.titus.common.util.tuple.Either;
import com.netflix.titus.master.kubernetes.client.model.PodWrapper;
import com.netflix.titus.runtime.kubernetes.KubeConstants;
import com.netflix.titus.testkit.model.job.JobGenerator;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import org.junit.Before;
import org.junit.Test;

import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_LOCAL_SYSTEM_ERROR;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_NORMAL;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_TASK_KILLED;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_TASK_LOST;
import static com.netflix.titus.master.kubernetes.PodDataGenerator.andDeletionTimestamp;
import static com.netflix.titus.master.kubernetes.PodDataGenerator.andMessage;
import static com.netflix.titus.master.kubernetes.PodDataGenerator.andPhase;
import static com.netflix.titus.master.kubernetes.PodDataGenerator.andReason;
import static com.netflix.titus.master.kubernetes.PodDataGenerator.andRunning;
import static com.netflix.titus.master.kubernetes.PodDataGenerator.andScheduled;
import static com.netflix.titus.master.kubernetes.PodDataGenerator.andTerminated;
import static com.netflix.titus.master.kubernetes.PodDataGenerator.andWaiting;
import static com.netflix.titus.master.kubernetes.PodDataGenerator.newPod;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PodToTaskMapperTest {

    private static final V1Node NODE = new V1Node().metadata(new V1ObjectMeta().name(NodeDataGenerator.NODE_NAME));

    private final TitusRuntime titusRuntime = TitusRuntimes.test();

    private final TestClock clock = (TestClock) titusRuntime.getClock();

    private final ContainerResultCodeResolver containerResultCodeResolver = mock(ContainerResultCodeResolver.class);

    private final KubernetesConfiguration configuration = Archaius2Ext.newConfiguration(KubernetesConfiguration.class);

    @Before
    public void setUp() throws Exception {
        when(containerResultCodeResolver.resolve(any(), any(), any())).thenAnswer(invocation -> {
            PodWrapper podWrapper = invocation.getArgument(2);
            String message = podWrapper.getMessage();
            if (message.contains("system")) {
                return Optional.of(TaskStatus.REASON_LOCAL_SYSTEM_ERROR);
            }
            return Optional.empty();
        });
    }

    @Test
    public void testUpdatesIgnoredWhenTaskFinished() {
        Task task = newTask(TaskState.Finished);
        V1Pod pod = newPod(andPhase("Pending"));
        Either<TaskStatus, String> result = updateMapper(task, pod).getNewTaskStatus();
        assertErrorMessage(result, "task already marked as finished");
    }

    @Test
    public void testPodCreated() {
        Task task = newTask(TaskState.Accepted);
        V1Pod pod = newPod(andPhase("Pending"));
        Either<TaskStatus, String> result = updateMapper(task, pod).getNewTaskStatus();
        assertErrorMessage(result, "pod notification does not change task state");
    }

    @Test
    public void testPodPendingAndScheduledButNotInWaitingState() {
        Task task = newTask(TaskState.Accepted);
        V1Pod pod = newPod(andPhase("Pending"), andMessage("junit"), andScheduled());
        Either<TaskStatus, String> result = updateMapper(task, pod).getNewTaskStatus();
        assertValue(result, TaskState.Launched, TaskStatus.REASON_POD_SCHEDULED);
    }

    @Test
    public void testPodScheduledAndLaunched() {
        Task task = newTask(TaskState.Accepted);
        V1Pod pod = newPod(andPhase("Pending"), andMessage("junit"), andWaiting(), andScheduled());
        Either<TaskStatus, String> result = updateMapper(task, pod).getNewTaskStatus();
        assertValue(result, TaskState.Launched, TaskStatus.REASON_POD_SCHEDULED);
    }

    @Test
    public void testPodScheduledAndStartInitiated() {
        Task task = newTask(TaskState.Launched);
        V1Pod pod = newPod(andPhase("Pending"), andMessage("junit"), andWaiting(), andScheduled(), andReason(PodToTaskMapper.TASK_STARTING));
        Either<TaskStatus, String> result = updateMapper(task, pod).getNewTaskStatus();
        assertValue(result, TaskState.StartInitiated, PodToTaskMapper.TASK_STARTING);
    }

    @Test
    public void testTaskStateAheadOfPodInPendingState() {
        Task task = newTask(TaskState.KillInitiated);
        V1Pod pod = newPod(andPhase("Pending"), andScheduled(), andWaiting(), andReason(PodToTaskMapper.TASK_STARTING));
        Either<TaskStatus, String> result = updateMapper(task, pod).getNewTaskStatus();
        assertErrorMessage(result, "pod in state not consistent with the task state");
    }

    @Test
    public void testPodRunning() {
        Task task = newTask(TaskState.StartInitiated);
        V1Pod pod = newPod(andPhase("Running"), andMessage("junit"), andScheduled(), andRunning(), andReason(TaskStatus.REASON_NORMAL));
        Either<TaskStatus, String> result = updateMapper(task, pod).getNewTaskStatus();
        assertValue(result, TaskState.Started, TaskStatus.REASON_NORMAL);
    }

    @Test
    public void testTaskStateAheadOfPodInRunningState() {
        Task task = newTask(TaskState.KillInitiated);
        V1Pod pod = newPod(andPhase("Running"), andScheduled(), andRunning(), andReason(TaskStatus.REASON_NORMAL));
        Either<TaskStatus, String> result = updateMapper(task, pod).getNewTaskStatus();
        assertErrorMessage(result, "pod state (Running) not consistent with the task state");
    }

    @Test
    public void testPodSucceeded() {
        Task task = newTask(TaskState.Started);
        V1Pod pod = newPod(andPhase("Succeeded"), andMessage("junit"), andScheduled(), andTerminated(), andReason(TaskStatus.REASON_NORMAL));
        Either<TaskStatus, String> result = updateMapper(task, pod).getNewTaskStatus();
        assertValue(result, TaskState.Finished, TaskStatus.REASON_NORMAL);
    }

    @Test
    public void testPodFailed() {
        Task task = newTask(TaskState.Started);
        V1Pod pod = newPod(andPhase("Failed"), andMessage("junit"), andScheduled(), andTerminated(), andReason("exit -1"));
        Either<TaskStatus, String> result = updateMapper(task, pod).getNewTaskStatus();
        assertValue(result, TaskState.Finished, TaskStatus.REASON_FAILED);
    }

    @Test
    public void testPodFailedWithKillReason() {
        Task task = newTask(TaskState.Started);
        V1Pod pod = newPod(andPhase("Failed"), andMessage("junit"), andScheduled(), andTerminated(), andReason(REASON_TASK_KILLED), andDeletionTimestamp());
        Either<TaskStatus, String> result = updateMapper(task, pod).getNewTaskStatus();
        assertValue(result, TaskState.Finished, REASON_TASK_KILLED);
    }

    @Test
    public void testPodStuckInStateClassifiedAsTransientSystemError() {
        Task task = JobFunctions.changeTaskStatus(
                newTask(TaskState.KillInitiated),
                TaskStatus.newBuilder().withState(TaskState.KillInitiated).withReasonCode(TaskStatus.REASON_STUCK_IN_STATE).build()
        );
        V1Pod pod = newPod(andPhase("Succeeded"), andMessage("junit"), andScheduled(), andTerminated(), andReason("terminated"));
        Either<TaskStatus, String> result = updateMapper(task, pod).getNewTaskStatus();
        assertValue(result, TaskState.Finished, TaskStatus.REASON_TRANSIENT_SYSTEM_ERROR);
    }

    @Test
    public void testPodDeletedWhenNodeLost() {
        Task task = newTask(TaskState.Started);
        V1Pod pod = newPod(andPhase("Running"), andReason(KubeConstants.NODE_LOST));
        Either<TaskStatus, String> result = deleteMapper(task, pod).getNewTaskStatus();
        assertValue(result, TaskState.Finished, REASON_TASK_KILLED, "The host running the container was unexpectedly terminated");
    }

    @Test
    public void testPodDeletedWhenUnexpectedlyTerminated() {
        Task task = newTask(TaskState.Started);
        V1Pod pod = newPod(andPhase("Running"), andReason("kubectl_terminate"));
        Either<TaskStatus, String> result = deleteMapper(task, pod).getNewTaskStatus();
        assertValue(result, TaskState.Finished, REASON_TASK_KILLED, "Container was terminated without going through the Titus API");
    }

    @Test
    public void testPodPendingDeletedWhenTaskStuckInState() {
        testPodDeletedWhenTaskStuckInState("Pending");
    }

    @Test
    public void testPodStartedDeletedWhenTaskStuckInState() {
        testPodDeletedWhenTaskStuckInState("Running");
    }

    @Test
    public void testPodSucceededDeletedWhenTaskStuckInState() {
        testPodDeletedWhenTaskStuckInState("Succeeded");
    }

    @Test
    public void testPodUpdateWithSystemErrorResolution() {
        Task task = newTask(TaskState.Started);
        V1Pod pod = newPod(andPhase("Failed"), andMessage("system error"));
        Either<TaskStatus, String> result = updateMapper(task, pod).getNewTaskStatus();
        assertValue(result, TaskState.Finished, REASON_LOCAL_SYSTEM_ERROR, "system error");
    }

    @Test
    public void testPodDeleteWithSystemErrorResolution() {
        Task task = newTask(TaskState.Started);
        V1Pod pod = newPod(andPhase("Running"), andMessage("system error"));
        Either<TaskStatus, String> result = deleteMapper(task, pod).getNewTaskStatus();
        assertValue(result, TaskState.Finished, REASON_LOCAL_SYSTEM_ERROR, "Container was terminated without going through the Titus API");
    }

    @Test
    public void testNodeLost() {
        Task task = newTask(TaskState.Accepted);
        V1Pod pod = newPod(
                andPhase("Running"),
                andReason("NodeLost"),
                andMessage("Node i-lostnode which was running pod lostpod is unresponsive")
        );

        // Check that the task is moved out of Accepted state first
        Either<TaskStatus, String> result1 = updateMapper(task, pod).getNewTaskStatus();
        assertValue(result1, TaskState.Launched, REASON_NORMAL,
                "The pod is scheduled but the communication with its node is lost. If not recovered in 10min, the task will be marked as failed"
        );

        // Now check that there is no change before the node lost deadline is reached.
        task = task.toBuilder().withStatus(result1.getValue()).build();
        Either<TaskStatus, String> result2 = updateMapper(task, pod).getNewTaskStatus();
        assertThat(result2.hasValue()).isTrue();
        assertThat(result2.getValue()).isEqualTo(result1.getValue());

        // Move time past the deadline
        clock.advanceTime(Duration.ofMillis(configuration.getNodeLostTimeoutMs()));
        Either<TaskStatus, String> result3 = updateMapper(task, pod).getNewTaskStatus();
        assertValue(result3, TaskState.Finished, REASON_TASK_LOST, "The node where task was scheduled is lost");
    }

    private void testPodDeletedWhenTaskStuckInState(String podPhase) {
        Task task = JobFunctions.changeTaskStatus(
                newTask(TaskState.KillInitiated),
                TaskStatus.newBuilder().withState(TaskState.KillInitiated).withReasonCode(TaskStatus.REASON_STUCK_IN_STATE).build()
        );
        V1Pod pod = newPod(andPhase(podPhase), andMessage("junit"), andScheduled());
        Either<TaskStatus, String> result = deleteMapper(task, pod).getNewTaskStatus();
        assertValue(result, TaskState.Finished, TaskStatus.REASON_TRANSIENT_SYSTEM_ERROR, "junit");
    }

    private Task newTask(TaskState taskState) {
        return JobFunctions.changeTaskStatus(JobGenerator.oneBatchTask(), TaskStatus.newBuilder().withState(taskState).build());
    }

    private PodToTaskMapper updateMapper(Task task, V1Pod v1Pod) {
        return new PodToTaskMapper(configuration, new PodWrapper(v1Pod), Optional.of(NODE), task, false, containerResultCodeResolver, titusRuntime);
    }

    private PodToTaskMapper deleteMapper(Task task, V1Pod v1Pod) {
        return new PodToTaskMapper(configuration, new PodWrapper(v1Pod), Optional.of(NODE), task, true, containerResultCodeResolver, titusRuntime);
    }

    private void assertValue(Either<TaskStatus, String> result, TaskState expectedState, String expectedReason) {
        assertValue(result, expectedState, expectedReason, "junit");
    }

    private void assertValue(Either<TaskStatus, String> result, TaskState expectedState, String expectedReason, String expectedReasonMessage) {
        assertThat(result.hasValue()).isTrue();
        assertThat(result.getValue().getState()).isEqualTo(expectedState);
        assertThat(result.getValue().getReasonCode()).isEqualTo(expectedReason);
        assertThat(result.getValue().getReasonMessage()).isEqualTo(expectedReasonMessage);
    }

    private void assertErrorMessage(Either<TaskStatus, String> result, String expected) {
        assertThat(result.hasError()).isTrue();
        assertThat(result.getError()).contains(expected);
    }
}