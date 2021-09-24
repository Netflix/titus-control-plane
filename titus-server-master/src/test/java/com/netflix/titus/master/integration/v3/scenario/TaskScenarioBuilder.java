/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.master.integration.v3.scenario;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.protobuf.Empty;
import com.netflix.fenzo.TaskRequest;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.common.aws.AwsInstanceType;
import com.netflix.titus.grpc.protogen.EvictionServiceGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.TaskAttributesDeleteRequest;
import com.netflix.titus.grpc.protogen.TaskAttributesUpdate;
import com.netflix.titus.grpc.protogen.TaskKillRequest;
import com.netflix.titus.grpc.protogen.TaskMoveRequest;
import com.netflix.titus.grpc.protogen.TaskStatus;
import com.netflix.titus.grpc.protogen.TaskTerminateRequest;
import com.netflix.titus.master.scheduler.SchedulingResultEvent;
import com.netflix.titus.master.scheduler.SchedulingService;
import com.netflix.titus.testkit.embedded.EmbeddedTitusOperations;
import com.netflix.titus.testkit.embedded.kube.EmbeddedKubeCluster;
import com.netflix.titus.testkit.embedded.kube.EmbeddedKubeNode;
import com.netflix.titus.testkit.embedded.kube.EmbeddedKubeUtil;
import com.netflix.titus.testkit.grpc.TestStreamObserver;
import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import io.kubernetes.client.openapi.models.V1Pod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;

import static com.jayway.awaitility.Awaitility.await;
import static com.netflix.titus.common.util.ExceptionExt.rethrow;
import static com.netflix.titus.master.integration.v3.scenario.ScenarioBuilderUtil.TIMEOUT_MS;
import static com.netflix.titus.master.integration.v3.scenario.ScenarioBuilderUtil.discoverActiveTest;
import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters.toCoreTaskState;
import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters.toGrpcTaskState;
import static java.util.Arrays.asList;

/**
 *
 */
public class TaskScenarioBuilder {

    private static final Logger logger = LoggerFactory.getLogger(TaskScenarioBuilder.class);

    private final JobManagementServiceGrpc.JobManagementServiceStub jobClient;
    private final EvictionServiceGrpc.EvictionServiceBlockingStub evictionClient;

    private final JobScenarioBuilder jobScenarioBuilder;

    private final ExtTestSubscriber<Task> eventStreamSubscriber = new ExtTestSubscriber<>();
    private final Subscription eventStreamSubscription;

    private final EmbeddedKubeCluster kubeCluster;
    private final SchedulingService<? extends TaskRequest> schedulingService;
    private final DiagnosticReporter diagnosticReporter;

    public TaskScenarioBuilder(EmbeddedTitusOperations titusOperations,
                               JobScenarioBuilder jobScenarioBuilder,
                               Observable<Task> eventStream,
                               SchedulingService<? extends TaskRequest> schedulingService,
                               DiagnosticReporter diagnosticReporter) {
        this.jobClient = titusOperations.getV3GrpcClient();
        this.kubeCluster = titusOperations.getKubeCluster();
        this.evictionClient = titusOperations.getBlockingGrpcEvictionClient();
        this.jobScenarioBuilder = jobScenarioBuilder;
        this.eventStreamSubscription = eventStream.subscribe(eventStreamSubscriber);
        this.schedulingService = schedulingService;
        this.diagnosticReporter = diagnosticReporter;
    }

    void stop() {
        eventStreamSubscription.unsubscribe();
    }

    public JobScenarioBuilder toJob() {
        return jobScenarioBuilder;
    }

    public Task getTask() {
        return eventStreamSubscriber.getLatestItem();
    }

    public TaskScenarioBuilder moveToKillInitiated() {
        return internalKill(false, false, false);
    }

    public TaskScenarioBuilder killTask() {
        return internalKill(false, false, true);
    }

    public TaskScenarioBuilder completeKillInitiated() {
        Task task = getTask();
        Preconditions.checkState(task.getStatus().getState() == TaskState.KillInitiated, "Expected task in KillInitiated state");
        kubeCluster.moveToFinishedSuccess(task.getId());
        return this;
    }

    public TaskScenarioBuilder killTaskAndShrink() {
        return internalKill(true, false, true);
    }

    public TaskScenarioBuilder killTaskAndShrinkWithMinCheck() {
        return internalKill(true, true, true);
    }

    private TaskScenarioBuilder internalKill(boolean shrink, boolean preventMinSizeUpdate, boolean moveToFinished) {
        String taskId = getTask().getId();
        logger.info("[{}] Killing task: jobId={}, taskId={}, shrink={}, preventMinSizeUpdate={}...", discoverActiveTest(),
                jobScenarioBuilder.getJobId(), taskId, shrink, preventMinSizeUpdate);
        Stopwatch stopWatch = Stopwatch.createStarted();

        TestStreamObserver<Empty> responseObserver = new TestStreamObserver<>();
        jobClient.killTask(
                TaskKillRequest.newBuilder()
                        .setTaskId(taskId)
                        .setShrink(shrink)
                        .setPreventMinSizeUpdate(preventMinSizeUpdate)
                        .build(),
                responseObserver
        );
        rethrow(() -> responseObserver.awaitDone(TIMEOUT_MS, TimeUnit.MILLISECONDS));

        expectStateUpdates(TaskStatus.TaskState.KillInitiated);
        if (moveToFinished) {
            kubeCluster.moveToFinishedSuccess(taskId);
        }

        logger.info("[{}] Task {} killed in {}[ms]", discoverActiveTest(), taskId, stopWatch.elapsed(TimeUnit.MILLISECONDS));
        return this;
    }

    public TaskScenarioBuilder updateTaskAttributes(Map<String, String> attributes) {
        String taskId = getTask().getId();
        logger.info("[{}] Updating attributes of task {} of job {}...", discoverActiveTest(), taskId, jobScenarioBuilder.getJobId());
        Stopwatch stopWatch = Stopwatch.createStarted();

        TestStreamObserver<Empty> responseObserver = new TestStreamObserver<>();
        jobClient.updateTaskAttributes(TaskAttributesUpdate.newBuilder().setTaskId(taskId).putAllAttributes(attributes).build(), responseObserver);
        rethrow(() -> responseObserver.awaitDone(TIMEOUT_MS, TimeUnit.MILLISECONDS));

        logger.info("[{}] Task {} updated in {}[ms]", discoverActiveTest(), taskId, stopWatch.elapsed(TimeUnit.MILLISECONDS));
        return this;
    }

    public TaskScenarioBuilder deleteTaskAttributes(List<String> keys) {
        String taskId = getTask().getId();
        logger.info("[{}] Deleting attributes of task {} of job {}...", discoverActiveTest(), taskId, jobScenarioBuilder.getJobId());
        Stopwatch stopWatch = Stopwatch.createStarted();

        TestStreamObserver<Empty> responseObserver = new TestStreamObserver<>();
        jobClient.deleteTaskAttributes(TaskAttributesDeleteRequest.newBuilder().setTaskId(taskId).addAllKeys(keys).build(), responseObserver);
        rethrow(() -> responseObserver.awaitDone(TIMEOUT_MS, TimeUnit.MILLISECONDS));

        logger.info("[{}] Task {} updated in {}[ms]", discoverActiveTest(), taskId, stopWatch.elapsed(TimeUnit.MILLISECONDS));
        return this;
    }

    public TaskScenarioBuilder evictTask() {
        String taskId = getTask().getId();
        logger.info("[{}] Evicting task {} of job {}...", discoverActiveTest(), taskId, jobScenarioBuilder.getJobId());
        Stopwatch stopWatch = Stopwatch.createStarted();

        evictionClient.terminateTask(TaskTerminateRequest.newBuilder().setTaskId(taskId).setReason("Test").build());

        logger.info("[{}] Task {} evicted in {}[ms]", discoverActiveTest(), taskId, stopWatch.elapsed(TimeUnit.MILLISECONDS));
        return this;
    }

    public TaskScenarioBuilder moveTask(String targetJobId) {
        String taskId = getTask().getId();
        logger.info("[{}] Moving Task {} to another job {}", discoverActiveTest(), taskId, targetJobId);
        Stopwatch stopWatch = Stopwatch.createStarted();

        TestStreamObserver<Empty> responseObserver = new TestStreamObserver<>();
        jobClient.moveTask(TaskMoveRequest.newBuilder()
                        .setSourceJobId(jobScenarioBuilder.getJobId())
                        .setTargetJobId(targetJobId)
                        .setTaskId(taskId)
                        .build(),
                responseObserver);
        rethrow(() -> responseObserver.awaitDone(TIMEOUT_MS, TimeUnit.MILLISECONDS));

        logger.info("[{}] Task {} moved to job {} in {}[ms]", discoverActiveTest(), taskId, targetJobId, stopWatch.elapsed(TimeUnit.MILLISECONDS));
        return this;
    }

    public TaskScenarioBuilder template(Function<TaskScenarioBuilder, TaskScenarioBuilder> templateFun) {
        return templateFun.apply(this);
    }

    public TaskScenarioBuilder failTaskExecution() {
        logger.info("[{}] Transition task to failed state", discoverActiveTest());
        kubeCluster.moveToFinishedFailed(getTask().getId(), "Simulated task execution failure");
        return this;
    }

    public TaskScenarioBuilder transitionToFailed() {
        logger.info("[{}] Moving task to failed state", discoverActiveTest());
        kubeCluster.moveToFinishedFailed(getTask().getId(), "marked as failed");
        return this;
    }

    public TaskScenarioBuilder transitionTo(TaskStatus.TaskState... taskStates) {
        logger.info("[{}] Transition task on agent through states {}", discoverActiveTest(), asList(taskStates));

        Task task = getTask();
        for (TaskStatus.TaskState taskState : taskStates) {
            if (taskState == TaskStatus.TaskState.StartInitiated) {
                kubeCluster.moveToStartInitiatedState(task.getId());
            } else if (taskState == TaskStatus.TaskState.Started) {
                kubeCluster.moveToStartedState(task.getId());
            } else if (taskState == TaskStatus.TaskState.KillInitiated) {
                kubeCluster.moveToKillInitiatedState(task.getId(), 0);
            } else if (taskState == TaskStatus.TaskState.Finished) {
                kubeCluster.moveToFinishedSuccess(task.getId());
            }
        }
        return this;
    }

    public TaskScenarioBuilder transitionUntil(TaskStatus.TaskState taskState) {
        logger.info("[{}] Transition task on agent to state {}", discoverActiveTest(), taskState);
        String taskId = getTask().getId();
        TaskState coreTaskState = EmbeddedKubeUtil.getPodState(kubeCluster.getPods().get(taskId));
        TaskStatus.TaskState currentState = toGrpcTaskState(coreTaskState);
        int startingPoint = currentState.ordinal();
        int targetPoint = taskState.ordinal();
        for (int next = startingPoint + 1; next <= targetPoint; next++) {
            TaskStatus.TaskState nextState = TaskStatus.TaskState.forNumber(next);
            if (nextState != TaskStatus.TaskState.KillInitiated) {
                if (nextState == TaskStatus.TaskState.StartInitiated) {
                    kubeCluster.moveToStartInitiatedState(taskId);
                } else if (nextState == TaskStatus.TaskState.Started) {
                    kubeCluster.moveToStartedState(taskId);
                } else if (nextState == TaskStatus.TaskState.Finished) {
                    kubeCluster.moveToFinishedSuccess(taskId);
                }
            }
        }
        return this;
    }

    public TaskScenarioBuilder expectAllTasksInKube() {
        Task task = getTask();
        logger.info("[{}] Expecting task {} Kube", discoverActiveTest(), task.getId());

        try {
            await().timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS).until(() -> kubeCluster.getPods().containsKey(task.getId()));
        } catch (Exception e) {
            diagnosticReporter.reportWhenTaskNotScheduled(task.getId());
            throw e;
        }
        return this;
    }

    public TaskScenarioBuilder expectTaskOnAgent() {
        Task task = getTask();
        logger.info("[{}] Expecting task {} on agent", discoverActiveTest(), task.getId());

        kubeCluster.schedule();
        expectStateAndReasonUpdateSkipOther(TaskStatus.TaskState.Launched, "SCHEDULED");
        kubeCluster.moveToStartInitiatedState(task.getId());
        return this;
    }


    public TaskScenarioBuilder expectSchedulingFailed() {
        String taskId = getTask().getId();
        logger.info("[{}] Expecting task {} to fail being scheduled", discoverActiveTest(), taskId);
        await().pollInterval(1, TimeUnit.SECONDS).timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS).until(() ->
                schedulingService.findLastSchedulingResult(taskId)
                        .map(event -> event instanceof SchedulingResultEvent.FailedSchedulingResultEvent)
                        .orElse(false)
        );
        return this;
    }

    public TaskScenarioBuilder expectInstanceType(AwsInstanceType expectedInstanceType) {
        logger.info("[{}] Expecting current task to run on instance type {}", discoverActiveTest(), expectedInstanceType);
        Task task = getTask();
        V1Pod pod = kubeCluster.getPods().get(task.getId());
        String nodeId = pod.getSpec().getNodeName();
        EmbeddedKubeNode node = kubeCluster.getFleet().getNodes().get(nodeId);
        String instanceType = node.getServerGroup().getInstanceType();
        Preconditions.checkArgument(
                expectedInstanceType.name().equalsIgnoreCase(instanceType),
                "Task is expected to run on AWS instance %s, but is running on %s", expectedInstanceType, instanceType
        );
        return this;
    }

    public TaskScenarioBuilder expectZoneId(String expectedZoneId) {
        logger.info("[{}] Expecting current task to run in zone {}", discoverActiveTest(), expectedZoneId);
        String taskId = getTask().getId();
        String zone = kubeCluster.getPlacementZone(taskId);
        Preconditions.checkArgument(
                zone.equalsIgnoreCase(expectedZoneId),
                "Task %s is expected to run in zone %s, but is running in %s (node %s)",
                taskId, expectedZoneId, zone, kubeCluster.getPods().get(taskId).getSpec().getNodeName()
        );
        return this;
    }

    public TaskScenarioBuilder expectTaskContext(String key, String value) {
        logger.info("[{}] Expecting current task to have taskContext entry {},{}", discoverActiveTest(), key, value);
        Preconditions.checkArgument(
                getTask().getTaskContext().getOrDefault(key, "").equals(value),
                "Task context does not contain {},{}", key, value
        );
        return this;
    }

    public TaskScenarioBuilder expectStaticIpAllocationInPod(String ipAllocationId) {
        logger.info("[{}] Expecting pod to have static IP allocation {}", discoverActiveTest(), ipAllocationId);
        String actual = kubeCluster.getEmbeddedPods().get(getTask().getId()).getStaticIpAllocation();
        Preconditions.checkArgument(
                ipAllocationId.equals(actual),
                "Different static IP allocations: expected={}, actual={}", ipAllocationId, actual
        );
        return this;
    }

    public TaskScenarioBuilder expectStateUpdates(TaskStatus.TaskState... expectedStates) {
        logger.info("[{}] Expecting sequence of events with task states {}...", discoverActiveTest(), asList(expectedStates));

        Stopwatch stopWatch = Stopwatch.createStarted();

        for (TaskStatus.TaskState expectedState : expectedStates) {
            TaskState expectedCoreState = toCoreTaskState(expectedState);
            expectTaskUpdate(task -> task.getStatus().getState() == expectedCoreState, "Expected state " + expectedCoreState);
        }

        logger.info("[{}] Expected sequence of events with task states {} received in {}[ms]", discoverActiveTest(), asList(expectedStates), stopWatch.elapsed(TimeUnit.MILLISECONDS));
        return this;
    }

    public TaskScenarioBuilder expectStateAndReasonUpdateSkipOther(TaskStatus.TaskState expectedState, String reasonCode) {
        logger.info("[{}] Expecting task state and reason {}/{} (skipping other)...", discoverActiveTest(), expectedState, reasonCode);

        Stopwatch stopWatch = Stopwatch.createStarted();
        TaskState expectedCoreState = toCoreTaskState(expectedState);
        expectTaskUpdate(
                task -> task.getStatus().getState().ordinal() <= expectedCoreState.ordinal(),
                task -> {
                    com.netflix.titus.api.jobmanager.model.job.TaskStatus taskStatus = task.getStatus();
                    return taskStatus.getState() == expectedCoreState && Objects.equals(reasonCode, taskStatus.getReasonCode());
                },
                "Expected state: " + expectedCoreState
        );

        logger.info("[{}] Expected task state {} received in {}[ms]", discoverActiveTest(), expectedState, stopWatch.elapsed(TimeUnit.MILLISECONDS));
        return this;
    }

    public TaskScenarioBuilder expectStateUpdateSkipOther(TaskStatus.TaskState expectedState) {
        logger.info("[{}] Expecting task state {} (skipping other)...", discoverActiveTest(), expectedState);

        Stopwatch stopWatch = Stopwatch.createStarted();
        TaskState expectedCoreState = toCoreTaskState(expectedState);
        expectTaskUpdate(
                task -> task.getStatus().getState().ordinal() < expectedCoreState.ordinal(),
                task -> task.getStatus().getState() == expectedCoreState,
                "Expected state: " + expectedCoreState
        );

        logger.info("[{}] Expected task state {} received in {}[ms]", discoverActiveTest(), expectedState, stopWatch.elapsed(TimeUnit.MILLISECONDS));
        return this;
    }

    public TaskScenarioBuilder assertTaskUpdate(Consumer<Task> assertFun) {
        Task task = expectTaskUpdate(update -> true, update -> true, "N/A");
        assertFun.accept(task);
        return this;
    }

    public TaskScenarioBuilder andThen(Runnable action) {
        action.run();
        return this;
    }

    private Task expectTaskUpdate(Predicate<Task> matcher, String message) {
        return expectTaskUpdate(t -> false, matcher, message);
    }

    private Task expectTaskUpdate(Predicate<Task> filter, Predicate<Task> matcher, String message) {
        while (true) {
            Task task = rethrow(() -> eventStreamSubscriber.takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS));
            Preconditions.checkNotNull(task, "No change notification received while waiting for task update event. %s", message);

            if (matcher.test(task)) {
                return task;
            }
            Preconditions.checkState(filter.test(task), "Received task state update with unexpected status value %s. %s", task.getStatus(), message);
        }
    }
}
