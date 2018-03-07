/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.integration.v3.scenario;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.TaskKillRequest;
import com.netflix.titus.grpc.protogen.TaskStatus;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.common.aws.AwsInstanceType;
import io.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
import io.netflix.titus.testkit.embedded.EmbeddedTitusOperations;
import io.netflix.titus.testkit.embedded.cloud.agent.TaskExecutorHolder;
import io.netflix.titus.testkit.grpc.TestStreamObserver;
import io.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.TaskStatus.Reason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;

import static com.jayway.awaitility.Awaitility.await;
import static io.netflix.titus.common.util.ExceptionExt.rethrow;
import static io.netflix.titus.master.integration.v3.scenario.ScenarioBuilderUtil.TIMEOUT_MS;
import static io.netflix.titus.master.integration.v3.scenario.ScenarioBuilderUtil.discoverActiveTest;
import static io.netflix.titus.master.integration.v3.scenario.ScenarioBuilderUtil.toMesosTaskState;
import static io.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_V2_TASK_ID;
import static io.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters.toCoreTaskState;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;

/**
 */
public class TaskScenarioBuilder {

    private static final Logger logger = LoggerFactory.getLogger(TaskScenarioBuilder.class);

    private final JobManagementServiceGrpc.JobManagementServiceStub client;
    private final JobScenarioBuilder jobScenarioBuilder;

    private final ExtTestSubscriber<Task> eventStreamSubscriber = new ExtTestSubscriber<>();
    private final Subscription eventStreamSubscription;

    private volatile TaskExecutorHolder taskExecutionHolder;

    public TaskScenarioBuilder(EmbeddedTitusOperations titusOperations, JobScenarioBuilder jobScenarioBuilder, Observable<Task> eventStream) {
        this.client = titusOperations.getV3GrpcClient();
        this.jobScenarioBuilder = jobScenarioBuilder;
        this.eventStreamSubscription = eventStream.subscribe(eventStreamSubscriber);
        eventStream.take(1).flatMap(task -> {
            String effectiveTaskId = task.getTaskContext().getOrDefault(TASK_ATTRIBUTES_V2_TASK_ID, task.getId());
            return titusOperations.awaitTaskExecutorHolderOf(effectiveTaskId);
        }).subscribe(holder -> {
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
        String taskId = getTask().getId();
        logger.info("[{}] Killing task {} of job {}...", discoverActiveTest(), taskId, jobScenarioBuilder.getJobId());
        Stopwatch stopWatch = Stopwatch.createStarted();

        TestStreamObserver<Empty> responseObserver = new TestStreamObserver<>();
        client.killTask(TaskKillRequest.newBuilder().setTaskId(taskId).build(), responseObserver);
        rethrow(responseObserver::awaitDone);

        logger.info("[{}] Task {} killed in {}[ms]", discoverActiveTest(), taskId, stopWatch.elapsed(TimeUnit.MILLISECONDS));
        return this;
    }

    public TaskScenarioBuilder killTaskAndShrink() {
        String taskId = getTask().getId();
        logger.info("[{}] Killing task {} of job and shrinking the job {}...", discoverActiveTest(), taskId, jobScenarioBuilder.getJobId());
        Stopwatch stopWatch = Stopwatch.createStarted();

        TestStreamObserver<Empty> responseObserver = new TestStreamObserver<>();
        client.killTask(TaskKillRequest.newBuilder().setTaskId(taskId).setShrink(true).build(), responseObserver);
        rethrow(responseObserver::awaitDone);

        logger.info("[{}] Task {} killed in {}[ms]", discoverActiveTest(), taskId, stopWatch.elapsed(TimeUnit.MILLISECONDS));
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

        await().timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS).until(() -> taskExecutionHolder != null);
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
        logger.info("[{}] Asserting task {}...", discoverActiveTest());

        Preconditions.checkArgument(predicate.test(getTask()), message);
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
