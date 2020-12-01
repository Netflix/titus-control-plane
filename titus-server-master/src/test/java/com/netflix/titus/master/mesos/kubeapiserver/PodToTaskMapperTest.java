/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.master.mesos.kubeapiserver;

import java.util.Collections;
import java.util.Optional;

import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.tuple.Either;
import com.netflix.titus.master.mesos.kubeapiserver.direct.model.PodWrapper;
import com.netflix.titus.runtime.kubernetes.KubeConstants;
import com.netflix.titus.testkit.model.job.JobGenerator;
import io.kubernetes.client.openapi.models.V1ContainerState;
import io.kubernetes.client.openapi.models.V1ContainerStateRunning;
import io.kubernetes.client.openapi.models.V1ContainerStateTerminated;
import io.kubernetes.client.openapi.models.V1ContainerStateWaiting;
import io.kubernetes.client.openapi.models.V1ContainerStatus;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodStatus;
import org.junit.Before;
import org.junit.Test;

import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_LOCAL_SYSTEM_ERROR;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_TASK_KILLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PodToTaskMapperTest {

    private static final String NODE_NAME = "node1";

    private static final V1Node NODE = new V1Node().metadata(new V1ObjectMeta().name(NODE_NAME));

    private final TitusRuntime titusRuntime = TitusRuntimes.test();

    private final ContainerResultCodeResolver containerResultCodeResolver = mock(ContainerResultCodeResolver.class);

    @Before
    public void setUp() throws Exception {
        when(containerResultCodeResolver.resolve(any(), any())).thenAnswer(invocation -> {
            String reason = invocation.getArgument(1);
            if (reason.contains("system")) {
                return Optional.of(TaskStatus.REASON_LOCAL_SYSTEM_ERROR);
            }
            return Optional.empty();
        });
    }

    @Test
    public void testUpdatesIgnoredWhenTaskFinished() {
        Task task = newTask(TaskState.Finished);
        V1Pod pod = newPod("Pending");
        Either<TaskStatus, String> result = updateMapper(task, pod).getNewTaskStatus();
        assertErrorMessage(result, "task already marked as finished");
    }

    @Test
    public void testPodCreated() {
        Task task = newTask(TaskState.Accepted);
        V1Pod pod = newPod("Pending");
        Either<TaskStatus, String> result = updateMapper(task, pod).getNewTaskStatus();
        assertErrorMessage(result, "pod notification does not change task state");
    }

    @Test
    public void testPodPendingAndScheduledButNotInWaitingState() {
        Task task = newTask(TaskState.Accepted);
        V1Pod pod = andScheduled(newPod("Pending"));
        Either<TaskStatus, String> result = updateMapper(task, pod).getNewTaskStatus();
        assertValue(result, TaskState.Launched, TaskStatus.REASON_POD_SCHEDULED);
    }

    @Test
    public void testPodScheduledAndLaunched() {
        Task task = newTask(TaskState.Accepted);
        V1Pod pod = andWaiting(andScheduled(newPod("Pending")));
        Either<TaskStatus, String> result = updateMapper(task, pod).getNewTaskStatus();
        assertValue(result, TaskState.Launched, TaskStatus.REASON_POD_SCHEDULED);
    }

    @Test
    public void testPodScheduledAndStartInitiated() {
        Task task = newTask(TaskState.Launched);
        V1Pod pod = andReason(andWaiting(andScheduled(newPod("Pending"))), PodToTaskMapper.TASK_STARTING);
        Either<TaskStatus, String> result = updateMapper(task, pod).getNewTaskStatus();
        assertValue(result, TaskState.StartInitiated, PodToTaskMapper.TASK_STARTING);
    }

    @Test
    public void testTaskStateAheadOfPodInPendingState() {
        Task task = newTask(TaskState.KillInitiated);
        V1Pod pod = andReason(andWaiting(andScheduled(newPod("Pending"))), PodToTaskMapper.TASK_STARTING);
        Either<TaskStatus, String> result = updateMapper(task, pod).getNewTaskStatus();
        assertErrorMessage(result, "pod in state not consistent with the task state");
    }

    @Test
    public void testPodRunning() {
        Task task = newTask(TaskState.StartInitiated);
        V1Pod pod = andReason(andRunning(andScheduled(newPod("Running"))), TaskStatus.REASON_NORMAL);
        Either<TaskStatus, String> result = updateMapper(task, pod).getNewTaskStatus();
        assertValue(result, TaskState.Started, TaskStatus.REASON_NORMAL);
    }

    @Test
    public void testTaskStateAheadOfPodInRunningState() {
        Task task = newTask(TaskState.KillInitiated);
        V1Pod pod = andReason(andRunning(andScheduled(newPod("Running"))), TaskStatus.REASON_NORMAL);
        Either<TaskStatus, String> result = updateMapper(task, pod).getNewTaskStatus();
        assertErrorMessage(result, "pod state (Running) not consistent with the task state");
    }

    @Test
    public void testPodSucceeded() {
        Task task = newTask(TaskState.Started);
        V1Pod pod = andReason(andTerminated(andScheduled(newPod("Succeeded"))), TaskStatus.REASON_NORMAL);
        Either<TaskStatus, String> result = updateMapper(task, pod).getNewTaskStatus();
        assertValue(result, TaskState.Finished, TaskStatus.REASON_NORMAL);
    }

    @Test
    public void testPodFailed() {
        Task task = newTask(TaskState.Started);
        V1Pod pod = andReason(andTerminated(andScheduled(newPod("Failed"))), "exit -1");
        Either<TaskStatus, String> result = updateMapper(task, pod).getNewTaskStatus();
        assertValue(result, TaskState.Finished, TaskStatus.REASON_FAILED);
    }

    @Test
    public void testPodStuckInStateClassifiedAsTransientSystemError() {
        Task task = JobFunctions.changeTaskStatus(
                newTask(TaskState.KillInitiated),
                TaskStatus.newBuilder().withState(TaskState.KillInitiated).withReasonCode(TaskStatus.REASON_STUCK_IN_STATE).build()
        );
        V1Pod pod = andReason(andTerminated(andScheduled(newPod("Succeeded"))), "terminated");
        Either<TaskStatus, String> result = updateMapper(task, pod).getNewTaskStatus();
        assertValue(result, TaskState.Finished, TaskStatus.REASON_TRANSIENT_SYSTEM_ERROR);
    }

    @Test
    public void testPodDeletedWhenNodeLost() {
        Task task = newTask(TaskState.Started);
        V1Pod pod = andReason(newPod("Running"), KubeConstants.NODE_LOST);
        Either<TaskStatus, String> result = deleteMapper(task, pod).getNewTaskStatus();
        assertValue(result, TaskState.Finished, REASON_TASK_KILLED, "The host running the container was unexpectedly terminated");
    }

    @Test
    public void testPodDeletedWhenUnexpectedlyTerminated() {
        Task task = newTask(TaskState.Started);
        V1Pod pod = andReason(newPod("Running"), "kubectl_terminate");
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
        V1Pod pod = andMessage(newPod("Failed"), "system error");
        Either<TaskStatus, String> result = updateMapper(task, pod).getNewTaskStatus();
        assertValue(result, TaskState.Finished, REASON_LOCAL_SYSTEM_ERROR, "system error");
    }

    @Test
    public void testPodDeleteWithSystemErrorResolution() {
        Task task = newTask(TaskState.Started);
        V1Pod pod = andMessage(newPod("Running"), "system error");
        Either<TaskStatus, String> result = deleteMapper(task, pod).getNewTaskStatus();
        assertValue(result, TaskState.Finished, REASON_LOCAL_SYSTEM_ERROR, "Container was terminated without going through the Titus API");
    }

    private void testPodDeletedWhenTaskStuckInState(String podPhase) {
        Task task = JobFunctions.changeTaskStatus(
                newTask(TaskState.KillInitiated),
                TaskStatus.newBuilder().withState(TaskState.KillInitiated).withReasonCode(TaskStatus.REASON_STUCK_IN_STATE).build()
        );
        V1Pod pod = andScheduled(newPod(podPhase));
        Either<TaskStatus, String> result = deleteMapper(task, pod).getNewTaskStatus();
        assertValue(result, TaskState.Finished, TaskStatus.REASON_TRANSIENT_SYSTEM_ERROR, "Container was terminated without going through the Titus API");
    }

    private Task newTask(TaskState taskState) {
        return JobFunctions.changeTaskStatus(JobGenerator.oneBatchTask(), TaskStatus.newBuilder().withState(taskState).build());
    }

    private V1Pod newPod(String podPhase) {
        return new V1Pod()
                .spec(new V1PodSpec())
                .status(new V1PodStatus()
                        .phase(podPhase)
                        .message("junit")
                );
    }

    private V1Pod andScheduled(V1Pod pod) {
        pod.getSpec().nodeName(NODE_NAME);
        return pod;
    }

    private V1Pod andWaiting(V1Pod pod) {
        pod.getStatus().containerStatuses(Collections.singletonList(
                new V1ContainerStatus().state(new V1ContainerState().waiting(new V1ContainerStateWaiting()))
        ));
        return pod;
    }

    private V1Pod andRunning(V1Pod pod) {
        pod.getStatus().containerStatuses(Collections.singletonList(
                new V1ContainerStatus().state(new V1ContainerState().running(new V1ContainerStateRunning()))
        ));
        return pod;
    }

    private V1Pod andTerminated(V1Pod pod) {
        pod.getStatus().containerStatuses(Collections.singletonList(
                new V1ContainerStatus().state(new V1ContainerState().terminated(new V1ContainerStateTerminated()))
        ));
        return pod;
    }

    private V1Pod andReason(V1Pod pod, String reason) {
        pod.getStatus().reason(reason);
        return pod;
    }

    private V1Pod andMessage(V1Pod pod, String message) {
        pod.getStatus().message(message);
        return pod;
    }

    private PodToTaskMapper updateMapper(Task task, V1Pod v1Pod) {
        return new PodToTaskMapper(new PodWrapper(v1Pod), Optional.of(NODE), task, false, containerResultCodeResolver, titusRuntime);
    }

    private PodToTaskMapper deleteMapper(Task task, V1Pod v1Pod) {
        return new PodToTaskMapper(new PodWrapper(v1Pod), Optional.of(NODE), task, true, containerResultCodeResolver, titusRuntime);
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