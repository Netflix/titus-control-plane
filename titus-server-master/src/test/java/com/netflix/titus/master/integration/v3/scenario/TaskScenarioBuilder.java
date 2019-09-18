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
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.protobuf.Empty;
import com.netflix.fenzo.TaskRequest;
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
import com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
import com.netflix.titus.testkit.embedded.EmbeddedTitusOperations;
import com.netflix.titus.testkit.embedded.cloud.agent.TaskExecutorHolder;
import com.netflix.titus.testkit.grpc.TestStreamObserver;
import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.TaskStatus.Reason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;

import static com.jayway.awaitility.Awaitility.await;
import static com.netflix.titus.common.util.ExceptionExt.rethrow;
import static com.netflix.titus.master.integration.v3.scenario.ScenarioBuilderUtil.TIMEOUT_MS;
import static com.netflix.titus.master.integration.v3.scenario.ScenarioBuilderUtil.discoverActiveTest;
import static com.netflix.titus.master.integration.v3.scenario.ScenarioBuilderUtil.toMesosTaskState;
import static com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters.toCoreTaskState;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;

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

    private final SchedulingService<? extends TaskRequest> schedulingService;
    private final DiagnosticReporter diagnosticReporter;

    private volatile TaskExecutorHolder taskExecutionHolder;

    public TaskScenarioBuilder(EmbeddedTitusOperations titusOperations,
                               JobScenarioBuilder jobScenarioBuilder,
                               Observable<Task> eventStream,
                               SchedulingService<? extends TaskRequest> schedulingService,
                               DiagnosticReporter diagnosticReporter) {
        this.jobClient = titusOperations.getV3GrpcClient();
        this.evictionClient = titusOperations.getBlockingGrpcEvictionClient();
        this.jobScenarioBuilder = jobScenarioBuilder;
        this.eventStreamSubscription = eventStream.subscribe(eventStreamSubscriber);
        this.schedulingService = schedulingService;
        this.diagnosticReporter = diagnosticReporter;
        eventStream.take(1).flatMap(task ->
                titusOperations.awaitTaskExecutorHolderOf(task.getId())
        ).subscribe(holder -> {
            taskExecutionHolder = holder;
            logger.info("TaskExecutorHolder set for task {}", holder.getTaskId());
        });
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

    public boolean hasTaskExecutorHolder() {
        return taskExecutionHolder != null;
    }

    public TaskExecutorHolder getTaskExecutionHolder() {
        Preconditions.checkNotNull(taskExecutionHolder, "Task is not running yet");
        return taskExecutionHolder;
    }

    public TaskScenarioBuilder killTask() {
        return internalKill(false, false);
    }

    public TaskScenarioBuilder killTaskAndShrink() {
        return internalKill(true, false);
    }

    public TaskScenarioBuilder killTaskAndShrinkWithMinCheck() {
        return internalKill(true, true);
    }

    private TaskScenarioBuilder internalKill(boolean shrink, boolean preventMinSizeUpdate) {
        String taskId = getTask().getId();
        logger.info("[{}] Killing task: jobId={}, taskId={}, shrink={}, xx={}...", discoverActiveTest(),
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
        taskExecutionHolder.transitionTo(Protos.TaskState.TASK_FAILED, Reason.REASON_COMMAND_EXECUTOR_FAILED, "Simulated task execution failure");
        return this;
    }

    public TaskScenarioBuilder transitionTo(TaskStatus.TaskState... taskStates) {
        logger.info("[{}] Transition task on agent through states {}", discoverActiveTest(), asList(taskStates));

        for (TaskStatus.TaskState taskState : taskStates) {
            getTaskExecutionHolder().transitionTo(toMesosTaskState(taskState));
        }
        return this;
    }

    public TaskScenarioBuilder transitionTo(Protos.TaskState... taskStates) {
        logger.info("[{}] Transition task on agent through states {}", discoverActiveTest(), asList(taskStates));

        for (Protos.TaskState taskState : taskStates) {
            getTaskExecutionHolder().transitionTo(taskState);
        }
        return this;
    }

    public TaskScenarioBuilder transitionUntil(TaskStatus.TaskState taskState) {
        logger.info("[{}] Transition task on agent to state {}", discoverActiveTest(), taskState);

        TaskExecutorHolder taskHolder = getTaskExecutionHolder();

        TaskStatus.TaskState currentState = ScenarioBuilderUtil.fromMesosTaskState(taskHolder.getTaskStatus().getState());
        int startingPoint = currentState.ordinal();
        int targetPoint = taskState.ordinal();
        for (int next = startingPoint + 1; next <= targetPoint; next++) {
            TaskStatus.TaskState nextState = TaskStatus.TaskState.forNumber(next);
            if (nextState != TaskStatus.TaskState.KillInitiated && nextState != TaskStatus.TaskState.Disconnected) {
                taskHolder.transitionTo(ScenarioBuilderUtil.toMesosTaskState(nextState));
            }
        }
        return this;
    }

    public TaskScenarioBuilder expectTaskOnAgent() {
        logger.info("[{}] Expecting task {} on agent", discoverActiveTest(), getTask().getId());

        try {
            await().timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS).until(() -> taskExecutionHolder != null);
        } catch (Exception e) {
            diagnosticReporter.reportWhenTaskNotScheduled(getTask().getId());
            throw e;
        }
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
        Preconditions.checkArgument(
                getTaskExecutionHolder().getInstanceType() == expectedInstanceType,
                "Task is expected to run on AWS instance %s, but is running on %s", expectedInstanceType, getTaskExecutionHolder().getInstanceType()
        );
        return this;
    }

    public TaskScenarioBuilder expectZoneId(String expectedZoneId) {
        logger.info("[{}] Expecting current task to run in zone {}", discoverActiveTest(), expectedZoneId);
        Preconditions.checkArgument(
                getTaskExecutionHolder().getAgent().getZoneId().equals(expectedZoneId),
                "Task is expected to run in zone %s, but is running on %s", expectedZoneId, getTaskExecutionHolder().getAgent().getZoneId()
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

    public TaskScenarioBuilder expectStateUpdateSkipOther(TaskStatus.TaskState expectedState) {
        logger.info("[{}] Expecting task state {} (skipping other)...", discoverActiveTest(), expectedState);

        Stopwatch stopWatch = Stopwatch.createStarted();
        TaskState expectedCoreState = toCoreTaskState(expectedState);
        expectTaskUpdate(task -> task.getStatus().getState().ordinal() < expectedCoreState.ordinal(), task -> task.getStatus().getState() == expectedCoreState, "Expected state: " + expectedCoreState);

        logger.info("[{}] Expected task state {} received in {}[ms]", discoverActiveTest(), expectedState, stopWatch.elapsed(TimeUnit.MILLISECONDS));
        return this;
    }

    public TaskScenarioBuilder expectOneOfStateUpdates(TaskStatus.TaskState... taskStates) {
        logger.info("[{}] Expecting task state from set {}...", discoverActiveTest(), asList(taskStates));

        Set<TaskState> coreTaskStates = stream(taskStates).map(V3GrpcModelConverters::toCoreTaskState).collect(Collectors.toSet());

        Stopwatch stopWatch = Stopwatch.createStarted();
        Task receivedTask = expectTaskUpdate(task -> coreTaskStates.contains(task.getStatus().getState()), "Expecting one of task states " + coreTaskStates);

        logger.info("[{}] Expected task state {} received in {}[ms]", discoverActiveTest(), receivedTask.getStatus().getState(), stopWatch.elapsed(TimeUnit.MILLISECONDS));
        return this;
    }

    public TaskScenarioBuilder assertTask(Predicate<Task> predicate, String message) {
        logger.info("[{}] Asserting task {}...", discoverActiveTest(), message);

        Preconditions.checkArgument(predicate.test(getTask()), message);
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
