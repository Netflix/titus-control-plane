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
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.JobModel;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import com.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.jobmanager.service.V3JobOperations.Trigger;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.jobmanager.service.JobManagerUtil;
import com.netflix.titus.master.jobmanager.service.integration.scenario.StubbedJobStore.StoreEvent;
import com.netflix.titus.master.mesos.model.ContainerStatus;
import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import rx.Subscriber;
import rx.schedulers.TestScheduler;

import static com.netflix.titus.master.jobmanager.service.integration.scenario.JobsScenarioBuilder.RECONCILER_ACTIVE_TIMEOUT_MS;
import static org.assertj.core.api.Assertions.assertThat;

public class JobScenarioBuilder<E extends JobDescriptor.JobDescriptorExt> {

    private final String jobId;
    private final TitusRuntime titusRuntime;
    private final EventHolder<JobManagerEvent<?>> jobEventsSubscriber;
    private final EventHolder<Pair<StoreEvent, ?>> storeEventsSubscriber;
    private final EventHolder<Pair<StubbedVirtualMachineMasterService.MesosEvent, String>> mesosEventsSubscriber;

    private final V3JobOperations jobOperations;
    private final StubbedSchedulingService schedulingService;
    private final StubbedJobStore jobStore;
    private final StubbedVirtualMachineMasterService vmService;
    private final TestScheduler testScheduler;
    private final boolean batchJob;

    public JobScenarioBuilder(String jobId,
                              EventHolder<JobManagerEvent<?>> jobEventsSubscriber,
                              EventHolder<Pair<StoreEvent, ?>> storeEventsSubscriber,
                              V3JobOperations jobOperations,
                              StubbedSchedulingService schedulingService,
                              StubbedJobStore jobStore,
                              StubbedVirtualMachineMasterService vmService,
                              TitusRuntime titusRuntime,
                              TestScheduler testScheduler) {
        this.jobId = jobId;
        this.titusRuntime = titusRuntime;
        this.batchJob = JobFunctions.isBatchJob(jobStore.retrieveJob(jobId).toBlocking().first());
        this.jobEventsSubscriber = jobEventsSubscriber;
        this.storeEventsSubscriber = storeEventsSubscriber;
        this.jobOperations = jobOperations;
        this.schedulingService = schedulingService;
        this.jobStore = jobStore;
        this.vmService = vmService;
        this.testScheduler = testScheduler;

        this.mesosEventsSubscriber = new EventHolder<>(this.jobStore);

        vmService.events().filter(pair -> jobOperations.findTaskById(pair.getRight()).isPresent()).subscribe(mesosEventsSubscriber);
    }

    public JobScenarioBuilder<E> advance() {
        testScheduler.advanceTimeBy(RECONCILER_ACTIVE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        return this;
    }

    public JobScenarioBuilder<E> andThen(Runnable action) {
        action.run();
        return this;
    }

    public JobScenarioBuilder<E> expectFailure(Callable<?> action, Consumer<Throwable> errorEvaluator) {
        try {
            action.call();
            throw new IllegalStateException("Expected action to fail");
        } catch (Exception e) {
            errorEvaluator.accept(e);
        }
        return this;
    }

    public JobScenarioBuilder<E> advance(long time, TimeUnit timeUnit) {
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

    public JobScenarioBuilder<E> inActiveTasks(BiFunction<Integer, Integer, Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>>> templateFun) {
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

    public JobScenarioBuilder<E> allActiveTasks(Consumer<Task> consumer) {
        List<Task> activeTasks = jobOperations.getTasks(jobId);
        activeTasks.forEach(consumer);
        return this;
    }

    public JobScenarioBuilder<E> allTasks(Consumer<List<Task>> templateFun) {
        templateFun.accept(jobOperations.getTasks(jobId));
        return this;
    }

    public JobScenarioBuilder<E> inAllTasks(Collection<Task> tasks, BiFunction<Integer, Integer, JobScenarioBuilder<E>> templateFun) {
        tasks.forEach(task -> templateFun.apply(jobStore.getIndex(task.getId()), task.getResubmitNumber()));
        return this;
    }

    public JobScenarioBuilder<E> firstTaskMatch(Predicate<Task> predicate, Consumer<Task> consumer) {
        Task task = jobOperations.getTasks(jobId).stream().filter(predicate).findFirst().orElseThrow(() -> new IllegalStateException("No task matches the given predicate"));
        consumer.accept(task);
        return this;
    }

    public JobScenarioBuilder<E> template(Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>> templateFun) {
        return templateFun.apply(this);
    }

    public JobScenarioBuilder<E> ignoreAvailableEvents() {
        jobEventsSubscriber.ignoreAvailableEvents();
        storeEventsSubscriber.ignoreAvailableEvents();
        return this;
    }

    public JobScenarioBuilder<E> changeCapacity(int min, int desired, int max) {
        return changeCapacity(Capacity.newBuilder().withMin(min).withDesired(desired).withMax(max).build());
    }

    public JobScenarioBuilder<E> changeCapacity(Capacity newCapacity) {
        ExtTestSubscriber<Void> subscriber = new ExtTestSubscriber<>();
        jobOperations.updateJobCapacity(jobId, newCapacity).subscribe(subscriber);

        autoAdvanceUntilSuccessful(() -> checkOperationSubscriberAndThrowExceptionIfError(subscriber));

        return this;
    }

    public JobScenarioBuilder<E> changeJobEnabledStatus(boolean enabled) {
        ExtTestSubscriber<Void> subscriber = new ExtTestSubscriber<>();
        jobOperations.updateJobStatus(jobId, enabled).subscribe(subscriber);

        autoAdvanceUntilSuccessful(() -> checkOperationSubscriberAndThrowExceptionIfError(subscriber));

        return this;
    }

    public JobScenarioBuilder<E> killJob() {
        ExtTestSubscriber<Void> subscriber = new ExtTestSubscriber<>();
        jobOperations.killJob(jobId).subscribe(subscriber);

        autoAdvanceUntilSuccessful(() -> checkOperationSubscriberAndThrowExceptionIfError(subscriber));

        return this;
    }

    public JobScenarioBuilder<E> killTask(Task task) {
        ExtTestSubscriber<Void> subscriber = new ExtTestSubscriber<>();
        jobOperations.killTask(task.getId(), false, "Task kill requested by a user").subscribe(subscriber);

        autoAdvanceUntilSuccessful(() -> checkOperationSubscriberAndThrowExceptionIfError(subscriber));

        return this;
    }

    public JobScenarioBuilder<E> killTask(int taskIdx, int resubmit) {
        return killTask(findTaskInActiveState(taskIdx, resubmit));
    }

    public JobScenarioBuilder<E> killTaskAndShrink(Task task) {
        ExtTestSubscriber<Void> subscriber = new ExtTestSubscriber<>();
        jobOperations.killTask(task.getId(), true, "Task terminate & shrink requested by a user").subscribe(subscriber);

        autoAdvanceUntilSuccessful(() -> checkOperationSubscriberAndThrowExceptionIfError(subscriber));

        return this;
    }

    public JobScenarioBuilder<E> killTaskAndShrink(int taskIdx, int resubmit) {
        return killTaskAndShrink(findTaskInActiveState(taskIdx, resubmit));
    }

    public JobScenarioBuilder<E> assertServiceJob(Consumer<Job<ServiceJobExt>> serviceJob) {
        Job<?> job = jobOperations.getJob(jobId).orElseThrow(() -> new IllegalStateException("Unknown job: " + jobId));
        assertThat(JobFunctions.isServiceJob(job)).describedAs("Not a service job: %s", jobId).isTrue();
        serviceJob.accept((Job<ServiceJobExt>) job);
        return this;
    }

    public JobScenarioBuilder<E> expectTaskInActiveState(int taskIdx, int resubmit, TaskState taskState) {
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

    public JobScenarioBuilder<E> expectJobEvent() {
        return expectJobEvent(job -> {
        });
    }

    public JobScenarioBuilder<E> expectNoJobStateChangeEvent() {
        JobManagerEvent<?> event = autoAdvance(jobEventsSubscriber::takeNextJobEvent);
        assertThat(event).isNull();
        return this;
    }

    public JobScenarioBuilder<ServiceJobExt> expectServiceJobEvent(Consumer<Job<ServiceJobExt>> check) {
        Preconditions.checkState(!batchJob, "Service job expected");
        Consumer checkNoTypeParam = check;
        return expectJobEvent(checkNoTypeParam);
    }

    public JobScenarioBuilder<E> expectJobEvent(Consumer<Job<?>> check) {
        JobManagerEvent<?> event = autoAdvance(jobEventsSubscriber::takeNextJobEvent);
        assertThat(event).describedAs("No job update event for job: %s", jobId).isNotNull();
        assertThat(event).isInstanceOf(JobUpdateEvent.class);

        JobUpdateEvent jobUpdateEvent = (JobUpdateEvent) event;
        assertThat(jobUpdateEvent.getCurrent().getId()).isEqualTo(jobId);
        check.accept(jobUpdateEvent.getCurrent());

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

    public JobScenarioBuilder<E> expectTaskEvent(int taskIdx, int resubmit, Consumer<TaskUpdateEvent> validator) {
        TaskUpdateEvent event = expectTaskEvent(taskIdx, resubmit);
        validator.accept(event);
        return this;
    }

    public JobScenarioBuilder<E> expectTaskStateChangeEvent(int taskIdx, int resubmit, TaskState taskState) {
        return expectTaskStateChangeEvent(taskIdx, resubmit, taskState, TaskStatus.REASON_NORMAL);
    }

    public JobScenarioBuilder<E> expectTaskStateChangeEvent(int taskIdx, int resubmit, TaskState taskState, String reasonCode) {
        TaskUpdateEvent event = expectTaskEvent(taskIdx, resubmit);

        TaskStatus status = event.getCurrent().getStatus();
        assertThat(status.getState()).isEqualTo(taskState);
        assertThat(reasonCode.equals(status.getReasonCode()));

        return this;
    }

    public JobScenarioBuilder<E> expectNoTaskStateChangeEvent() {
        JobManagerEvent<?> event = autoAdvance(jobEventsSubscriber::takeNextTaskEvent);
        assertThat(event).isNull();
        return this;
    }

    public JobScenarioBuilder<E> expectScheduleRequest(int taskIdx, int resubmit) {
        Task task = jobStore.expectTaskInStore(jobId, taskIdx, resubmit);

        assertThat(schedulingService.getQueuableTasks().get(task.getId()))
                .describedAs("Task %s (index %d) is not scheduled yet", task.getId(), taskIdx)
                .isNotNull();
        return this;
    }

    public JobScenarioBuilder<E> expectServiceJobUpdatedInStore(Consumer<Job<ServiceJobExt>> check) {
        Preconditions.checkState(!batchJob, "Service job expected");
        Consumer checkNoTypeParam = check;
        return expectJobUpdatedInStore(checkNoTypeParam);
    }

    public JobScenarioBuilder<E> expectJobUpdatedInStore(Consumer<Job<?>> check) {
        Pair<StoreEvent, ?> storeEventPair = autoAdvance((storeEventsSubscriber::takeNextJobStoreEvent));
        assertThat(storeEventPair.getLeft()).isEqualTo(StoreEvent.JobUpdated);

        Job<?> job = (Job<?>) storeEventPair.getRight();
        check.accept(job);

        return this;
    }

    public JobScenarioBuilder<E> expectJobArchivedInStore() {
        Pair<StoreEvent, ?> storeEventPair = storeEventsSubscriber.takeNextJobStoreEvent();
        assertThat(storeEventPair.getLeft()).isEqualTo(StoreEvent.JobRemoved);
        return this;
    }

    public JobScenarioBuilder<E> expectTaskAddedToStore(int taskIdx, int resubmit, Consumer<Task> check) {
        Task task = expectTaskEvent(taskIdx, resubmit, StoreEvent.TaskAdded);
        check.accept(task);
        return this;
    }

    public JobScenarioBuilder<E> expectTaskUpdatedInStore(int taskIdx, int resubmit, Consumer<Task> check) {
        Task task = expectTaskEvent(taskIdx, resubmit, StoreEvent.TaskUpdated);
        check.accept(task);
        return this;
    }

    public JobScenarioBuilder<E> expectedTaskArchivedInStore(int taskIdx, int resubmit) {
        expectTaskEvent(taskIdx, resubmit, StoreEvent.TaskRemoved);
        return this;
    }

    public JobScenarioBuilder<E> expectNoStoreUpdate(int taskIdx, int resubmit) {
        Pair<StoreEvent, ?> event = autoAdvance(() -> storeEventsSubscriber.takeNextTaskStoreEvent(taskIdx, resubmit));
        assertThat(event).isNull();
        return this;
    }

    public JobScenarioBuilder<E> expectNoStoreUpdate() {
        Pair<StoreEvent, ?> event = autoAdvance(storeEventsSubscriber::takeNext);
        assertThat(event).isNull();
        return this;
    }

    public JobScenarioBuilder<E> expectMesosTaskKill(int taskIdx, int resubmit) {
        Task task = findTaskInActiveState(taskIdx, resubmit);
        Pair<StubbedVirtualMachineMasterService.MesosEvent, String> mesosEvent = mesosEventsSubscriber.takeNextMesosEvent(taskIdx, resubmit);
        assertThat(mesosEvent).describedAs("Expected task kill sent to Mesos").isNotNull();
        assertThat(mesosEvent.getLeft()).isEqualTo(StubbedVirtualMachineMasterService.MesosEvent.TaskKillRequest);
        assertThat(mesosEvent.getRight()).isEqualTo(task.getId());
        return this;
    }

    public JobScenarioBuilder<E> expectNoMesosEvent() {
        Pair<StubbedVirtualMachineMasterService.MesosEvent, String> event = autoAdvance(mesosEventsSubscriber::takeNext);
        assertThat(event).isNull();
        return this;
    }

    public JobScenarioBuilder<E> triggerSchedulerLaunchEvent(int taskIdx, int resubmit) {
        Task task = findTaskInActiveState(taskIdx, resubmit);

        Function<Task, Task> changeFunction = JobManagerUtil.newTaskLaunchConfigurationUpdater(
                "zone",
                vmService.buildLease(task.getId()),
                vmService.buildConsumeResult(task.getId()),
                Optional.empty(),
                vmService.buildAttributesMap(task.getId())
        );

        AtomicBoolean done = new AtomicBoolean();
        AtomicReference<Throwable> failed = new AtomicReference<>();
        jobOperations.recordTaskPlacement(task.getId(), changeFunction).subscribe(() -> done.set(true), failed::set);
        autoAdvanceUntil(() -> failed.get() != null || done.get());
        if (failed.get() != null) {
            ExceptionExt.rethrow(failed.get());
        }
        assertThat(done.get()).isTrue();

        return this;
    }

    public JobScenarioBuilder<E> triggerFailingSchedulerLaunchEvent(int taskIdx, int resubmit, Consumer<Throwable> assertFun) {
        Task task = findTaskInActiveState(taskIdx, resubmit);

        Function<Task, Task> changeFunction = JobManagerUtil.newTaskLaunchConfigurationUpdater(
                "zone",
                vmService.buildLease(task.getId()),
                vmService.buildConsumeResult(task.getId()),
                Optional.empty(),
                vmService.buildAttributesMap(task.getId())
        );

        AtomicReference<Throwable> failed = new AtomicReference<>();
        jobOperations.recordTaskPlacement(task.getId(), changeFunction).subscribe(
                () -> {
                },
                failed::set
        );
        autoAdvanceUntil(() -> failed.get() != null);
        assertThat(failed.get()).isNotNull();
        assertFun.accept(failed.get());

        return this;
    }

    public JobScenarioBuilder<E> triggerMesosLaunchEvent(int taskIdx, int resubmit) {
        return triggerMesosEvent(taskIdx, resubmit, TaskState.Launched, "Task launched", 0);
    }

    public JobScenarioBuilder<E> triggerMesosStartInitiatedEvent(int taskIdx, int resubmit) {
        return triggerMesosEvent(taskIdx, resubmit, TaskState.StartInitiated, "Starting container", 0);
    }

    public JobScenarioBuilder<E> triggerMesosStartedEvent(int taskIdx, int resubmit) {
        return triggerMesosEvent(taskIdx, resubmit, TaskState.Started, "Task started", 0);
    }

    public JobScenarioBuilder<E> triggerMesosFinishedEvent(int taskIdx, int resubmit) {
        return triggerMesosEvent(taskIdx, resubmit, TaskState.Finished, TaskStatus.REASON_NORMAL, 0);
    }

    public JobScenarioBuilder<E> triggerMesosFinishedEvent(Task task, int errorCode, String reasonCode) {
        return triggerMesosEvent(task, TaskState.Finished, reasonCode, errorCode);
    }

    public JobScenarioBuilder<E> triggerMesosFinishedEvent(int taskIdx, int resubmit, int errorCode, String reasonCode) {
        return triggerMesosEvent(taskIdx, resubmit, TaskState.Finished, reasonCode, errorCode);
    }

    public JobScenarioBuilder<E> breakStore() {
        jobStore.setBroken(true);
        return this;
    }

    public JobScenarioBuilder<E> enableStore() {
        jobStore.setBroken(false);
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

    private JobScenarioBuilder<E> triggerMesosEvent(int taskIdx, int resubmit, TaskState taskState, String reason, int errorCode) {
        Task task = jobStore.expectTaskInStore(jobId, taskIdx, resubmit);
        return triggerMesosEvent(task, taskState, reason, errorCode);
    }

    private JobScenarioBuilder<E> triggerMesosEvent(Task task, TaskState taskState, String reason, int errorCode) {
        String reasonMessage;
        if (taskState == TaskState.Finished) {
            reasonMessage = errorCode == 0 ? "Completed successfully" : "Container terminated with an error " + errorCode;
        } else {
            reasonMessage = "Task changed state to " + taskState;
        }

        AtomicBoolean done = new AtomicBoolean();
        List<ContainerStatus> data = taskState == TaskState.StartInitiated
                ? Collections.singletonList(vmService.buildNetworkConfigurationUpdate(task.getId()))
                : Collections.emptyList();

        TaskStatus taskStatus = JobModel.newTaskStatus()
                .withState(taskState)
                .withReasonCode(reason)
                .withReasonMessage(reasonMessage)
                .withTimestamp(testScheduler.now())
                .build();

        Function<Task, Optional<Task>> changeFunction = JobManagerUtil.newMesosTaskStateUpdater(taskStatus, data, titusRuntime);

        jobOperations.updateTask(task.getId(),
                changeFunction,
                Trigger.Mesos,
                String.format("Mesos callback taskStatus=%s, reason=%s (%s)", taskState, reason, reasonMessage)
        ).subscribe(() -> done.set(true));
        autoAdvanceUntil(done::get);
        assertThat(done.get()).isTrue();

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

    String getJobId() {
        return jobId;
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

        public Pair<StubbedVirtualMachineMasterService.MesosEvent, String> takeNextMesosEvent(int taskIdx, int resubmit) {
            Iterator<EVENT> it = events.iterator();
            while (it.hasNext()) {
                Pair<StubbedVirtualMachineMasterService.MesosEvent, String> event = (Pair<StubbedVirtualMachineMasterService.MesosEvent, String>) it.next();
                Task task = jobStore.expectTaskInStoreOrStoreArchive(event.getRight());
                if (jobStore.hasIndexAndResubmit(task, taskIdx, resubmit)) {
                    it.remove();
                    return event;
                }
            }
            return null;
        }

        void ignoreAvailableEvents() {
            events.clear();
        }
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
