/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.titus.master.jobmanager.service.integration.scenario;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.CapacityAttributes;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.JobModel;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobProcesses;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudget;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import com.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.jobmanager.service.V3JobOperations.Trigger;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.api.model.callmetadata.Caller;
import com.netflix.titus.api.model.callmetadata.CallerType;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.jobmanager.service.JobManagerUtil;
import com.netflix.titus.master.jobmanager.service.VersionSupplier;
import com.netflix.titus.master.jobmanager.service.VersionSuppliers;
import com.netflix.titus.master.jobmanager.service.integration.scenario.StubbedJobStore.StoreEvent;
import com.netflix.titus.master.mesos.TitusExecutorDetails;
import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import com.netflix.titus.testkit.rx.TitusRxSubscriber;
import rx.Subscriber;
import rx.schedulers.TestScheduler;

import static com.netflix.titus.master.jobmanager.service.integration.scenario.JobsScenarioBuilder.RECONCILER_ACTIVE_TIMEOUT_MS;
import static org.assertj.core.api.Assertions.assertThat;

public class JobScenarioBuilder {

    static final CallMetadata CHANGE_CAPACITY_CALL_METADATA = CallMetadata.newBuilder()
            .withCallers(Collections.singletonList(Caller.newBuilder()
                    .withId("capacity")
                    .withCallerType(CallerType.Application)
                    .build()))
            .withCallReason("capacity update")
            .build();

    private final String jobId;
    private final VersionSupplier versionSupplier;
    private final TitusRuntime titusRuntime;
    private final EventHolder<JobManagerEvent<?>> jobEventsSubscriber;
    private final EventHolder<Pair<StoreEvent, ?>> storeEventsSubscriber;

    private final V3JobOperations jobOperations;
    private final StubbedJobStore jobStore;
    private final StubbedComputeProvider computeProvider;
    private final TestScheduler testScheduler;
    private final boolean batchJob;
    private final CallMetadata callMetadata = CallMetadata.newBuilder().withCallReason("Testing call metadata").withCallerId("test").build();

    public JobScenarioBuilder(String jobId,
                              EventHolder<JobManagerEvent<?>> jobEventsSubscriber,
                              EventHolder<Pair<StoreEvent, ?>> storeEventsSubscriber,
                              V3JobOperations jobOperations,
                              StubbedJobStore jobStore,
                              StubbedComputeProvider computeProvider,
                              VersionSupplier versionSupplier,
                              TitusRuntime titusRuntime,
                              TestScheduler testScheduler) {
        this.jobId = jobId;
        this.versionSupplier = versionSupplier;
        this.titusRuntime = titusRuntime;
        this.batchJob = JobFunctions.isBatchJob(jobStore.retrieveJob(jobId).toBlocking().first());
        this.jobEventsSubscriber = jobEventsSubscriber;
        this.storeEventsSubscriber = storeEventsSubscriber;
        this.jobOperations = jobOperations;
        this.jobStore = jobStore;
        this.computeProvider = computeProvider;
        this.testScheduler = testScheduler;
    }

    public String getJobId() {
        return jobId;
    }

    public JobScenarioBuilder enableKubeIntegration(boolean enabled) {
        computeProvider.enableScheduling(enabled);
        return this;
    }

    public StubbedComputeProvider getComputeProvider() {
        return computeProvider;
    }

    public JobScenarioBuilder advance() {
        testScheduler.advanceTimeBy(RECONCILER_ACTIVE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        return this;
    }

    public JobScenarioBuilder andThen(Runnable action) {
        action.run();
        return this;
    }

    public JobScenarioBuilder expectVersionsOrdered() {
        expectJobVersionsOrdered();
        expectTaskVersionsOrdered();
        return this;
    }

    public JobScenarioBuilder expectJobVersionsOrdered() {
        List<Job> revisions = jobStore.getJobRevisions(jobId);
        Job last = revisions.get(0);
        for (int i = 1; i < revisions.size(); i++) {
            Job next = revisions.get(i);
            assertThat(next.getVersion().getTimestamp()).isGreaterThanOrEqualTo(last.getVersion().getTimestamp());
        }
        return this;
    }

    public JobScenarioBuilder expectTaskVersionsOrdered() {
        Map<String, List<Task>> taskGroups = jobStore.getTaskRevisions(jobId);
        taskGroups.forEach((originalId, list) -> {
            Task last = list.get(0);
            for (int i = 1; i < list.size(); i++) {
                Task next = list.get(i);
                assertThat(next.getVersion().getTimestamp()).isGreaterThanOrEqualTo(last.getVersion().getTimestamp());
            }
        });
        return this;
    }

    public JobScenarioBuilder expectFailure(Callable<?> action, Consumer<Throwable> errorEvaluator) {
        try {
            action.call();
            throw new IllegalStateException("Expected action to fail");
        } catch (Exception e) {
            errorEvaluator.accept(e);
        }
        return this;
    }

    public JobScenarioBuilder advance(long time, TimeUnit timeUnit) {
        long timeMs = timeUnit.toMillis(time);
        long steps = timeMs / RECONCILER_ACTIVE_TIMEOUT_MS;
        if (steps > 0) {
            for (int i = 0; i < steps; i++) {
                advance();
            }
        }
        testScheduler.advanceTimeBy(timeMs - steps * RECONCILER_ACTIVE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        return this;
    }

    public JobScenarioBuilder inActiveTasks(BiFunction<Integer, Integer, Function<JobScenarioBuilder, JobScenarioBuilder>> templateFun) {
        List<Task> activeTasks = jobOperations.getTasks(jobId);
        activeTasks.forEach(task -> {
            if (task instanceof BatchJobTask) {
                BatchJobTask batchTask = (BatchJobTask) task;
                templateFun.apply(batchTask.getIndex(), batchTask.getResubmitNumber()).apply(this);
            } else {
                jobStore.getIndexAndResubmit(task.getId()).ifPresent(pair -> {
                    templateFun.apply(pair.getLeft(), task.getResubmitNumber()).apply(this);
                });
            }
        });
        return this;
    }

    public JobScenarioBuilder inTask(int taskIdx, int resubmit, Consumer<Task> consumer) {
        consumer.accept(findTaskInActiveState(taskIdx, resubmit));
        return this;
    }

    public JobScenarioBuilder allActiveTasks(Consumer<Task> consumer) {
        List<Task> activeTasks = jobOperations.getTasks(jobId);
        activeTasks.forEach(consumer);
        return this;
    }

    public JobScenarioBuilder allTasks(Consumer<List<Task>> templateFun) {
        templateFun.accept(jobOperations.getTasks(jobId));
        return this;
    }

    public JobScenarioBuilder inAllTasks(Collection<Task> tasks, BiFunction<Integer, Integer, JobScenarioBuilder> templateFun) {
        tasks.forEach(task -> templateFun.apply(jobStore.getIndex(task.getId()), task.getResubmitNumber()));
        return this;
    }

    public JobScenarioBuilder firstTaskMatch(Predicate<Task> predicate, Consumer<Task> consumer) {
        Task task = jobOperations.getTasks(jobId).stream().filter(predicate).findFirst().orElseThrow(() -> new IllegalStateException("No task matches the given predicate"));
        consumer.accept(task);
        return this;
    }

    public JobScenarioBuilder template(Function<JobScenarioBuilder, JobScenarioBuilder> templateFun) {
        return templateFun.apply(this);
    }

    public JobScenarioBuilder ignoreAvailableEvents() {
        jobEventsSubscriber.ignoreAvailableEvents();
        storeEventsSubscriber.ignoreAvailableEvents();
        return this;
    }

    public JobScenarioBuilder changeCapacity(int min, int desired, int max) {
        return changeCapacity(Capacity.newBuilder().withMin(min).withDesired(desired).withMax(max).build());
    }

    public JobScenarioBuilder changeCapacity(Capacity newCapacity) {
        ExtTestSubscriber<Void> subscriber = new ExtTestSubscriber<>();
        CapacityAttributes capacityAttributes = JobModel.newCapacityAttributes(newCapacity).build();
        jobOperations.updateJobCapacityAttributes(jobId, capacityAttributes, CHANGE_CAPACITY_CALL_METADATA).subscribe(subscriber);
        autoAdvanceUntilSuccessful(() -> checkOperationSubscriberAndThrowExceptionIfError(subscriber));
        return this;
    }

    public JobScenarioBuilder changeJobEnabledStatus(boolean enabled) {
        ExtTestSubscriber<Void> subscriber = new ExtTestSubscriber<>();
        jobOperations.updateJobStatus(jobId, enabled, callMetadata).subscribe(subscriber);

        autoAdvanceUntilSuccessful(() -> checkOperationSubscriberAndThrowExceptionIfError(subscriber));

        return this;
    }

    public JobScenarioBuilder changServiceJobProcesses(ServiceJobProcesses processes) {
        ExtTestSubscriber<Void> subscriber = new ExtTestSubscriber<>();
        jobOperations.updateServiceJobProcesses(jobId, processes, callMetadata).subscribe(subscriber);

        autoAdvanceUntilSuccessful(() -> checkOperationSubscriberAndThrowExceptionIfError(subscriber));

        return this;
    }

    public JobScenarioBuilder changeDisruptionBudget(DisruptionBudget disruptionBudget) {
        TitusRxSubscriber<Void> subscriber = new TitusRxSubscriber<>();
        jobOperations.updateJobDisruptionBudget(jobId, disruptionBudget, callMetadata).subscribe(subscriber);

        autoAdvanceUntilSuccessful(() -> checkOperationSubscriberAndThrowExceptionIfError(subscriber));

        return this;
    }

    public JobScenarioBuilder killJob() {
        ExtTestSubscriber<Void> subscriber = new ExtTestSubscriber<>();
        jobOperations.killJob(jobId, "Testing", callMetadata).subscribe(subscriber);

        autoAdvanceUntilSuccessful(() -> checkOperationSubscriberAndThrowExceptionIfError(subscriber));

        return this;
    }

    public JobScenarioBuilder killTask(Task task, Trigger trigger) {
        TitusRxSubscriber<Void> subscriber = new TitusRxSubscriber<>();
        jobOperations.killTask(task.getId(), false, false, trigger, callMetadata).subscribe(subscriber);

        autoAdvanceUntilSuccessful(() -> checkOperationSubscriberAndThrowExceptionIfError(subscriber));

        return this;
    }

    public JobScenarioBuilder killTask(int taskIdx, int resubmit, Trigger trigger) {
        return killTask(findTaskInActiveState(taskIdx, resubmit), trigger);
    }

    public JobScenarioBuilder killTask(int taskIdx, int resubmit) {
        return killTask(taskIdx, resubmit, Trigger.API);
    }

    public JobScenarioBuilder killTaskAndShrink(Task task) {
        TitusRxSubscriber<Void> subscriber = new TitusRxSubscriber<>();
        jobOperations.killTask(task.getId(), true, false, Trigger.API, callMetadata).subscribe(subscriber);

        autoAdvanceUntilSuccessful(() -> checkOperationSubscriberAndThrowExceptionIfError(subscriber));

        return this;
    }

    public JobScenarioBuilder killTaskAndShrinkNoWait(Task task) {
        TitusRxSubscriber<Void> subscriber = new TitusRxSubscriber<>();
        jobOperations.killTask(task.getId(), true, false, Trigger.API, callMetadata).subscribe(subscriber);
        return this;
    }

    public JobScenarioBuilder killTaskAndShrink(int taskIdx, int resubmit) {
        return killTaskAndShrink(findTaskInActiveState(taskIdx, resubmit));
    }

    public JobScenarioBuilder moveTask(int taskIdx, int resubmit, String sourceJobId, String targetJobId) {
        Task task = findTaskInActiveState(taskIdx, resubmit);

        ExtTestSubscriber<Void> subscriber = new ExtTestSubscriber<>();
        jobOperations.moveServiceTask(sourceJobId, targetJobId, task.getId(), callMetadata).subscribe(subscriber);

        autoAdvanceUntilSuccessful(() -> checkOperationSubscriberAndThrowExceptionIfError(subscriber));

        return this;
    }

    public JobScenarioBuilder failNextPodCreate(RuntimeException simulatedError) {
        computeProvider.failNextTaskLaunch(simulatedError);
        return this;
    }

    public JobScenarioBuilder assertServiceJob(Consumer<Job<ServiceJobExt>> serviceJob) {
        Job<?> job = jobOperations.getJob(jobId).orElseThrow(() -> new IllegalStateException("Unknown job: " + jobId));
        assertThat(JobFunctions.isServiceJob(job)).describedAs("Not a service job: %s", jobId).isTrue();
        serviceJob.accept((Job<ServiceJobExt>) job);
        return this;
    }

    public JobScenarioBuilder expectTaskInActiveState(int taskIdx, int resubmit, TaskState taskState) {
        Task task = findTaskInActiveState(taskIdx, resubmit);
        assertThat(task.getStatus().getState()).isEqualTo(taskState);

        return this;
    }

    private Task findTaskInActiveState(int taskIdx, int resubmit) {
        Task task = jobOperations.getTasks(jobId).stream()
                .filter(t -> jobStore.hasIndexAndResubmit(t, taskIdx, resubmit))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Job has no active task with index " + taskIdx));
        assertThat(task.getResubmitNumber()).isEqualTo(resubmit);
        return task;
    }

    public JobScenarioBuilder expectJobEvent() {
        return expectJobEvent(job -> {
        });
    }

    public JobScenarioBuilder expectNoJobStateChangeEvent() {
        JobManagerEvent<?> event = autoAdvance(jobEventsSubscriber::takeNextJobEvent);
        assertThat(event).isNull();
        return this;
    }

    public JobScenarioBuilder expectServiceJobEvent(Consumer<Job<ServiceJobExt>> check) {
        Preconditions.checkState(!batchJob, "Service job expected");
        Consumer checkNoTypeParam = check;
        return expectJobEvent(checkNoTypeParam);
    }

    public JobScenarioBuilder expectJobUpdateEventObject(Consumer<JobUpdateEvent> check) {
        JobManagerEvent<?> event = autoAdvance(jobEventsSubscriber::takeNextJobEvent);
        assertThat(event).describedAs("No job update event for job: %s", jobId).isNotNull();
        assertThat(event).isInstanceOf(JobUpdateEvent.class);
        check.accept((JobUpdateEvent) event);
        return this;
    }

    public JobScenarioBuilder expectJobEvent(Consumer<Job<?>> check) {
        expectJobUpdateEventObject(jobUpdateEvent -> {
            assertThat(jobUpdateEvent.getCurrent().getId()).isEqualTo(jobId);
            check.accept(jobUpdateEvent.getCurrent());
        });
        return this;
    }

    private TaskUpdateEvent expectTaskEvent(int taskIdx, int resubmit) {
        jobStore.expectTaskInStore(jobId, taskIdx, resubmit);

        JobManagerEvent<?> event = autoAdvance(() -> jobEventsSubscriber.takeNextTaskEvent(taskIdx, resubmit));
        assertThat(event).isNotNull();
        assertThat(event).isInstanceOf(TaskUpdateEvent.class);

        TaskUpdateEvent taskUpdateEvent = (TaskUpdateEvent) event;
        Task taskFromEvent = taskUpdateEvent.getCurrent();

        assertThat(jobStore.hasIndexAndResubmit(taskFromEvent, taskIdx, resubmit))
                .describedAs("Expected event for task index %i and resubmit %i, but got %s", taskIdx, resubmit, taskFromEvent.getId())
                .isTrue();

        return taskUpdateEvent;
    }

    public JobScenarioBuilder expectArchivedTaskEvent(int taskIdx, int resubmit) {
        JobManagerEvent<?> event = autoAdvance(() -> jobEventsSubscriber.takeNextTaskEvent(taskIdx, resubmit));
        assertThat(event).isNotNull();
        assertThat(event).isInstanceOf(TaskUpdateEvent.class);

        TaskUpdateEvent taskUpdateEvent = (TaskUpdateEvent) event;
        assertThat(taskUpdateEvent.isArchived()).isTrue();
        return this;
    }

    public JobScenarioBuilder expectTaskEvent(int taskIdx, int resubmit, Consumer<TaskUpdateEvent> validator) {
        TaskUpdateEvent event = expectTaskEvent(taskIdx, resubmit);
        validator.accept(event);
        return this;
    }

    public JobScenarioBuilder expectTaskStateChangeEvent(int taskIdx, int resubmit, TaskState taskState) {
        TaskUpdateEvent event = expectTaskEvent(taskIdx, resubmit);

        TaskStatus status = event.getCurrent().getStatus();
        assertThat(status.getState()).isEqualTo(taskState);

        return this;
    }

    public JobScenarioBuilder expectTaskStateChangeEvent(int taskIdx, int resubmit, TaskState taskState, String reasonCode) {
        TaskUpdateEvent event = expectTaskEvent(taskIdx, resubmit);

        TaskStatus status = event.getCurrent().getStatus();
        assertThat(status.getState()).isEqualTo(taskState);
        assertThat(status.getReasonCode()).isEqualTo(reasonCode);

        return this;
    }

    public JobScenarioBuilder expectNoTaskStateChangeEvent() {
        JobManagerEvent<?> event = autoAdvance(jobEventsSubscriber::takeNextTaskEvent);
        assertThat(event).isNull();
        return this;
    }

    public JobScenarioBuilder expectComputeProviderCreateRequest(int taskIdx, int resubmit) {
        Task task = jobStore.expectTaskInStore(jobId, taskIdx, resubmit);

        advance();
        assertThat(computeProvider.hasComputeProviderTask(task.getId()))
                .describedAs("Task %s (index %d) is not scheduled yet", task.getId(), taskIdx)
                .isTrue();
        expectTaskStateChangeEvent(taskIdx, resubmit, TaskState.Accepted, TaskStatus.REASON_POD_CREATED);
        expectTaskUpdatedInStore(taskIdx, resubmit, t -> assertThat(t.getStatus().getReasonCode()).isEqualTo(TaskStatus.REASON_POD_CREATED));

        return this;
    }

    public JobScenarioBuilder expectServiceJobUpdatedInStore(Consumer<Job<ServiceJobExt>> check) {
        Preconditions.checkState(!batchJob, "Service job expected");
        Consumer checkNoTypeParam = check;
        return expectJobUpdatedInStore(checkNoTypeParam);
    }

    public JobScenarioBuilder expectJobUpdatedInStore(Consumer<Job<?>> check) {
        Pair<StoreEvent, ?> storeEventPair = autoAdvance((storeEventsSubscriber::takeNextJobStoreEvent));
        assertThat(storeEventPair.getLeft()).isEqualTo(StoreEvent.JobUpdated);

        Job<?> job = (Job<?>) storeEventPair.getRight();
        check.accept(job);

        return this;
    }

    public JobScenarioBuilder expectJobArchivedInStore() {
        Pair<StoreEvent, ?> storeEventPair = storeEventsSubscriber.takeNextJobStoreEvent();
        assertThat(storeEventPair.getLeft()).isEqualTo(StoreEvent.JobRemoved);
        return this;
    }

    public JobScenarioBuilder expectTaskAddedToStore(int taskIdx, int resubmit, Consumer<Task> check) {
        Task task = expectTaskEvent(taskIdx, resubmit, StoreEvent.TaskAdded);
        check.accept(task);
        return this;
    }

    public JobScenarioBuilder expectTaskUpdatedInStore(int taskIdx, int resubmit, Consumer<Task> check) {
        Task task = expectTaskEvent(taskIdx, resubmit, StoreEvent.TaskUpdated);
        check.accept(task);
        return this;
    }

    public JobScenarioBuilder expectedTaskArchivedInStore(int taskIdx, int resubmit) {
        expectTaskEvent(taskIdx, resubmit, StoreEvent.TaskRemoved);
        return this;
    }

    public JobScenarioBuilder expectNoStoreUpdate(int taskIdx, int resubmit) {
        Pair<StoreEvent, ?> event = autoAdvance(() -> storeEventsSubscriber.takeNextTaskStoreEvent(taskIdx, resubmit));
        assertThat(event).isNull();
        return this;
    }

    public JobScenarioBuilder expectNoStoreUpdate() {
        Pair<StoreEvent, ?> event = autoAdvance(storeEventsSubscriber::takeNext);
        assertThat(event).isNull();
        return this;
    }

    public JobScenarioBuilder expectPodTerminated(int taskIdx, int resubmit) {
        Task task = findTaskInActiveState(taskIdx, resubmit);
        assertThat(computeProvider.hasComputeProviderTask(task.getId())).describedAs("Expected not to find task in Kube").isFalse();
        return this;
    }

    public JobScenarioBuilder expectComputeProviderTaskFinished(int taskIdx, int resubmit) {
        Task task = findTaskInActiveState(taskIdx, resubmit);
        assertThat(computeProvider.isTaskFinished(task.getId())).isTrue();
        return this;
    }

    public JobScenarioBuilder triggerSchedulerLaunchEvent(int taskIdx, int resubmit) {
        Task task = findTaskInActiveState(taskIdx, resubmit);
        computeProvider.scheduleTask(task.getId());
        triggerComputePlatformEvent(taskIdx, resubmit, TaskState.Launched, "scheduled", -1);
        return this;
    }

    public JobScenarioBuilder triggerComputePlatformLaunchEvent(int taskIdx, int resubmit) {
        return triggerComputePlatformEvent(taskIdx, resubmit, TaskState.Launched, "Task launched", 0);
    }

    public JobScenarioBuilder triggerComputePlatformStartInitiatedEvent(int taskIdx, int resubmit) {
        return triggerComputePlatformEvent(taskIdx, resubmit, TaskState.StartInitiated, "Starting container", 0);
    }

    public JobScenarioBuilder triggerComputePlatformStartedEvent(int taskIdx, int resubmit) {
        return triggerComputePlatformEvent(taskIdx, resubmit, TaskState.Started, "Task started", 0);
    }

    public JobScenarioBuilder triggerComputePlatformFinishedEvent(int taskIdx, int resubmit) {
        return triggerComputePlatformEvent(taskIdx, resubmit, TaskState.Finished, TaskStatus.REASON_NORMAL, 0);
    }

    public JobScenarioBuilder triggerComputePlatformFinishedEvent(Task task, int errorCode, String reasonCode) {
        return triggerComputePlatformEvent(task, TaskState.Finished, reasonCode, errorCode);
    }

    public JobScenarioBuilder triggerComputePlatformFinishedEvent(int taskIdx, int resubmit, int errorCode, String reasonCode) {
        return triggerComputePlatformEvent(taskIdx, resubmit, TaskState.Finished, reasonCode, errorCode);
    }

    public JobScenarioBuilder breakStore() {
        jobStore.setStoreState(StubbedJobStore.StoreState.Broken);
        return this;
    }

    public JobScenarioBuilder slowStore() {
        jobStore.setStoreState(StubbedJobStore.StoreState.Slow);
        return this;
    }

    public JobScenarioBuilder enableStore() {
        jobStore.setStoreState(StubbedJobStore.StoreState.Normal);
        return this;
    }

    private Task expectTaskEvent(int taskIdx, int resubmit, StoreEvent eventType) {
        Task lastInStore = autoAdvance(() -> eventType == StoreEvent.TaskRemoved
                ? jobStore.expectTaskInStoreArchive(jobId, taskIdx, resubmit)
                : jobStore.expectTaskInStore(jobId, taskIdx, resubmit)
        );

        Pair<StoreEvent, Task> storeEventPair = storeEventsSubscriber.takeNextTaskStoreEvent(taskIdx, resubmit);
        assertThat(storeEventPair.getLeft()).isEqualTo(eventType);

        Task task = storeEventPair.getRight();
        assertThat(task.getId())
                .describedAs("Task version in store different from the task in the store event: %s != %s", lastInStore.getId(), task.getId())
                .isEqualTo(lastInStore.getId());

        return task;
    }

    private JobScenarioBuilder triggerComputePlatformEvent(int taskIdx, int resubmit, TaskState taskState, String reason, int errorCode) {
        Task task = jobStore.expectTaskInStore(jobId, taskIdx, resubmit);
        return triggerComputePlatformEvent(task, taskState, reason, errorCode);
    }

    private JobScenarioBuilder triggerComputePlatformEvent(Task task, TaskState taskState, String reason, int errorCode) {
        String reasonMessage;
        if (taskState == TaskState.Finished) {
            reasonMessage = errorCode == 0 ? "Completed successfully" : "Container terminated with an error " + errorCode;
        } else {
            reasonMessage = "Task changed state to " + taskState;
        }

        AtomicBoolean done = new AtomicBoolean();
        Optional<TitusExecutorDetails> data = taskState == TaskState.StartInitiated
                ? Optional.of(computeProvider.buildExecutorDetails(task.getId()))
                : Optional.empty();

        final Map<String, String> newTaskContext = new HashMap<>();
        if (taskState == TaskState.Launched) {
            newTaskContext.putAll(computeProvider.getScheduledTaskContext(task.getId()));
        }

        TaskStatus taskStatus = JobModel.newTaskStatus()
                .withState(taskState)
                .withReasonCode(reason)
                .withReasonMessage(reasonMessage)
                .withTimestamp(testScheduler.now())
                .build();

        Function<Task, Optional<Task>> changeFunction = currentTask ->
                JobManagerUtil.newMesosTaskStateUpdater(taskStatus, data, titusRuntime)
                        .apply(currentTask)
                        .map(updated -> updated.toBuilder()
                                .withTaskContext(CollectionsExt.merge(updated.getTaskContext(), newTaskContext))
                                .build()
                        );

        jobOperations.updateTask(task.getId(),
                changeFunction,
                Trigger.ComputeProvider,
                String.format("ComputeProvider callback taskStatus=%s, reason=%s (%s)", taskState, reason, reasonMessage),
                callMetadata
        ).subscribe(() -> done.set(true));
        autoAdvanceUntil(done::get);
        assertThat(done.get()).isTrue();

        return this;
    }

    public JobScenarioBuilder modifyJobStoreRecord(Function<Job, Job> transformer) {
        Job<?> storedJob = jobStore.retrieveJob(jobId).toBlocking().first();
        Job updatedJob = VersionSuppliers.nextVersion(transformer.apply(storedJob), versionSupplier);
        assertThat(jobStore.updateJob(updatedJob).get()).isNull();

        return this;
    }

    private boolean autoAdvanceUntil(Supplier<Boolean> action) {
        for (int i = 0; i < 5; i++) {
            if (action.get()) {
                return true;
            }
            advance();
        }
        return false;
    }

    private <T> T autoAdvance(Supplier<T> action) {
        Optional<T> result;
        for (int i = 0; i < 5; i++) {
            result = ExceptionExt.doTry(action);
            if (result.isPresent()) {
                return result.get();
            }
            advance();
        }
        return null;
    }

    private void autoAdvanceUntilSuccessful(Runnable action) {
        for (int i = 0; i < 5; i++) {
            try {
                action.run();
                return;
            } catch (Throwable ignore) {
            }
            advance();
        }
        action.run();
    }

    Job getJob() {
        return jobOperations.getJob(jobId).orElseThrow(() -> new IllegalArgumentException("Job not found: " + jobId));
    }

    /**
     * Returns tasks in strict order.
     */
    List<Task> getActiveTasks() {
        return jobOperations.getTasks(jobId).stream().sorted((task1, task2) -> {
            if (task1 instanceof BatchJobTask) {
                BatchJobTask batchTask1 = (BatchJobTask) task1;
                BatchJobTask batchTask2 = (BatchJobTask) task2;
                return Integer.compare(batchTask1.getIndex(), batchTask2.getIndex());
            }
            int task1Index = jobStore.getIndexAndResubmit(task1.getId()).get().getLeft();
            int task2Index = jobStore.getIndexAndResubmit(task2.getId()).get().getLeft();
            return Integer.compare(task1Index, task2Index);
        }).collect(Collectors.toList());
    }

    static class EventHolder<EVENT> extends Subscriber<EVENT> {

        private final StubbedJobStore jobStore;
        private final List<EVENT> events = new ArrayList<>();

        EventHolder(StubbedJobStore jobStore) {
            this.jobStore = jobStore;
        }

        @Override
        public void onCompleted() {
        }

        @Override
        public void onError(Throwable e) {
        }

        @Override
        public void onNext(EVENT e) {
            events.add(e);
        }

        EVENT takeNext() {
            if (events.isEmpty()) {
                return null;
            }
            return events.remove(0);
        }

        Pair<StoreEvent, Job> takeNextJobStoreEvent() {
            Iterator<EVENT> it = events.iterator();
            while (it.hasNext()) {
                Pair<StoreEvent, ?> event = (Pair<StoreEvent, ?>) it.next();
                if (event.getRight() instanceof Job) {
                    it.remove();
                    return Pair.of(event.getLeft(), (Job) event.getRight());
                }
            }
            return null;
        }

        Pair<StoreEvent, Task> takeNextTaskStoreEvent(int index, int resubmit) {
            Iterator<EVENT> it = events.iterator();
            while (it.hasNext()) {
                Pair<StoreEvent, ?> event = (Pair<StoreEvent, ?>) it.next();
                if (event.getRight() instanceof Task) {
                    Task task = (Task) event.getRight();
                    if (jobStore.hasIndexAndResubmit(task, index, resubmit)) {
                        it.remove();
                        return Pair.of(event.getLeft(), task);
                    }
                }
            }
            return null;
        }

        JobUpdateEvent takeNextJobEvent() {
            Iterator<EVENT> it = events.iterator();
            while (it.hasNext()) {
                JobManagerEvent<?> event = (JobManagerEvent<?>) it.next();
                if (event instanceof JobUpdateEvent) {
                    it.remove();
                    return (JobUpdateEvent) event;
                }
            }
            return null;
        }

        TaskUpdateEvent takeNextTaskEvent() {
            Iterator<EVENT> it = events.iterator();
            while (it.hasNext()) {
                JobManagerEvent<?> event = (JobManagerEvent<?>) it.next();
                if (event instanceof TaskUpdateEvent) {
                    it.remove();
                    return (TaskUpdateEvent) event;
                }
            }
            return null;
        }

        public TaskUpdateEvent takeNextTaskEvent(int taskIdx, int resubmit) {
            Iterator<EVENT> it = events.iterator();
            while (it.hasNext()) {
                JobManagerEvent<?> event = (JobManagerEvent<?>) it.next();
                if (event instanceof TaskUpdateEvent) {
                    Task task = ((TaskUpdateEvent) event).getCurrentTask();
                    if (jobStore.hasIndexAndResubmit(task, taskIdx, resubmit)) {
                        it.remove();
                        return (TaskUpdateEvent) event;
                    }
                }
            }
            return null;
        }

        void ignoreAvailableEvents() {
            events.clear();
        }
    }

    private void checkOperationSubscriberAndThrowExceptionIfError(TitusRxSubscriber<Void> subscriber) {
        if (subscriber.hasError()) {
            Throwable error = subscriber.getError();
            if (error instanceof RuntimeException) {
                throw (RuntimeException) error;
            }
            throw new RuntimeException(error);
        }
        assertThat(subscriber.isOpen()).isFalse();
    }

    private void checkOperationSubscriberAndThrowExceptionIfError(ExtTestSubscriber<Void> subscriber) {
        Throwable error = subscriber.getError();
        if (error != null) {
            if (error instanceof RuntimeException) {
                throw (RuntimeException) error;
            }
            throw new RuntimeException(error);
        }
        subscriber.assertOnCompleted();
    }
}
