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

package io.netflix.titus.master.jobmanager.service.integration.scenario;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import io.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.JobModel;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import io.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import io.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.api.jobmanager.service.V3JobOperations.Trigger;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.jobmanager.service.JobManagerUtil;
import io.netflix.titus.master.jobmanager.service.integration.scenario.StubbedJobStore.StoreEvent;
import io.netflix.titus.master.jobmanager.service.integration.scenario.StubbedVirtualMachineMasterService.MesosEvent;
import io.netflix.titus.testkit.rx.ExtTestSubscriber;
import rx.schedulers.TestScheduler;

import static org.assertj.core.api.Assertions.assertThat;

public class JobScenarioBuilder<E extends JobDescriptor.JobDescriptorExt> {

    private final String jobId;
    private final ExtTestSubscriber<JobManagerEvent<?>> jobEventsSubscriber;
    private final ExtTestSubscriber<Pair<StoreEvent, ?>> storeEventsSubscriber;
    private final ExtTestSubscriber<Pair<MesosEvent, String>> mesosEventsSubscriber = new ExtTestSubscriber<>();

    private final V3JobOperations jobOperations;
    private final StubbedSchedulingService schedulingService;
    private final StubbedJobStore jobStore;
    private final StubbedVirtualMachineMasterService vmService;
    private final TestScheduler testScheduler;

    public JobScenarioBuilder(String jobId,
                              ExtTestSubscriber<JobManagerEvent<?>> jobEventsSubscriber,
                              ExtTestSubscriber<Pair<StoreEvent, ?>> storeEventsSubscriber,
                              V3JobOperations jobOperations,
                              StubbedSchedulingService schedulingService,
                              StubbedJobStore jobStore,
                              StubbedVirtualMachineMasterService vmService,
                              TestScheduler testScheduler) {
        this.jobId = jobId;
        this.jobEventsSubscriber = jobEventsSubscriber;
        this.storeEventsSubscriber = storeEventsSubscriber;
        this.jobOperations = jobOperations;
        this.schedulingService = schedulingService;
        this.jobStore = jobStore;
        this.vmService = vmService;
        this.testScheduler = testScheduler;

        vmService.events().filter(pair -> jobOperations.findTaskById(pair.getRight()).isPresent()).subscribe(mesosEventsSubscriber);
    }

    public JobScenarioBuilder<E> trigger() {
        testScheduler.triggerActions();
        return this;
    }

    public JobScenarioBuilder<E> advance() {
        testScheduler.advanceTimeBy(JobsScenarioBuilder.RECONCILER_ACTIVE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        return this;
    }

    public JobScenarioBuilder<E> advance(long time, TimeUnit timeUnit) {
        testScheduler.advanceTimeBy(time, timeUnit);
        return this;
    }

    public JobScenarioBuilder<E> template(Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>> templateFun) {
        return templateFun.apply(this);
    }

    public JobScenarioBuilder<E> ignoreAvailableEvents() {
        while (jobEventsSubscriber.takeNext() != null) {
        }
        while (storeEventsSubscriber.takeNext() != null) {
        }
        return this;
    }


    public JobScenarioBuilder<E> killTask(int taskIdx, int resubmit) {
        Task task = findTaskInActiveState(taskIdx, resubmit);
        AtomicBoolean done = new AtomicBoolean();
        jobOperations.killTask(task.getId(), false, "Task kill requested by a user")
                .doOnCompleted(() -> done.set(true)).subscribe();

        advance();
        assertThat(done.get()).isTrue();

        return this;
    }

    public JobScenarioBuilder<E> expectTaskInActiveState(int taskIdx, int resubmit, TaskState taskState) {
        Task task = findTaskInActiveState(taskIdx, resubmit);
        assertThat(task.getStatus().getState()).isEqualTo(taskState);

        return this;
    }

    private Task findTaskInActiveState(int taskIdx, int resubmit) {
        BatchJobTask task = (BatchJobTask) jobOperations.getTasks(jobId).stream()
                .filter(t -> ((BatchJobTask) t).getIndex() == taskIdx)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Job has no active task with index " + taskIdx));
        assertThat(task.getResubmitNumber()).isEqualTo(resubmit);
        return task;
    }

    public JobScenarioBuilder<E> expectJobEvent() {
        return expectJobEvent(job -> {
        });
    }

    public JobScenarioBuilder<E> expectJobEvent(Consumer<Job<?>> check) {
        JobManagerEvent<?> event = autoAdvance(jobEventsSubscriber::takeNext);
        assertThat(event).describedAs("No job update event for job: %s", jobId).isNotNull();
        assertThat(event).isInstanceOf(JobUpdateEvent.class);

        JobUpdateEvent jobUpdateEvent = (JobUpdateEvent) event;
        assertThat(jobUpdateEvent.getCurrent().getId()).isEqualTo(jobId);
        check.accept(jobUpdateEvent.getCurrent());

        return this;
    }

    private TaskUpdateEvent expectBatchTaskEvent(int taskIdx, int resubmit) {
        jobStore.expectTaskInStore(jobId, taskIdx, resubmit);

        JobManagerEvent<?> event = autoAdvance(jobEventsSubscriber::takeNext);
        assertThat(event).isNotNull();
        assertThat(event).isInstanceOf(TaskUpdateEvent.class);

        TaskUpdateEvent taskUpdateEvent = (TaskUpdateEvent) event;
        BatchJobTask taskFromEvent = (BatchJobTask) taskUpdateEvent.getCurrent();

        assertThat(taskFromEvent.getIndex())
                .describedAs("Expected event for task index %i, but got %i", taskIdx, taskFromEvent.getIndex())
                .isEqualTo(taskIdx);

        return taskUpdateEvent;
    }

    public JobScenarioBuilder<E> expectBatchTaskStateChangeEvent(int taskIdx, int resubmit, TaskState taskState) {
        return expectBatchTaskStateChangeEvent(taskIdx, resubmit, taskState, TaskStatus.REASON_NORMAL);
    }

    public JobScenarioBuilder<E> expectBatchTaskStateChangeEvent(int taskIdx, int resubmit, TaskState taskState, String reasonCode) {
        TaskUpdateEvent event = expectBatchTaskEvent(taskIdx, resubmit);

        TaskStatus status = event.getCurrent().getStatus();
        assertThat(status.getState()).isEqualTo(taskState);
        assertThat(reasonCode.equals(status.getReasonCode()));

        return this;
    }

    public JobScenarioBuilder<E> expectNoTaskStateChangeEvent() {
        JobManagerEvent<?> event = autoAdvance(jobEventsSubscriber::takeNext);
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

    public JobScenarioBuilder<E> expectJobUpdatedInStore(Consumer<Job<?>> check) {
        Pair<StoreEvent, ?> storeEventPair = storeEventsSubscriber.takeNext();
        assertThat(storeEventPair.getLeft()).isEqualTo(StoreEvent.JobUpdated);

        Job<?> job = (Job<?>) storeEventPair.getRight();
        check.accept(job);

        return this;
    }

    public JobScenarioBuilder<E> expectJobArchivedInStore() {
        Pair<StoreEvent, ?> storeEventPair = storeEventsSubscriber.takeNext();
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

    public JobScenarioBuilder<E> expectNoStoreUpdate() {
        Pair<StoreEvent, ?> event = autoAdvance(storeEventsSubscriber::takeNext);
        assertThat(event).isNull();
        return this;
    }

    public JobScenarioBuilder<E> expectMesosTaskKill(int taskIdx, int resubmit) {
        Task task = findTaskInActiveState(taskIdx, resubmit);
        Pair<MesosEvent, String> mesosEvent = mesosEventsSubscriber.takeNext();
        assertThat(mesosEvent).describedAs("Expected task kill sent to Mesos").isNotNull();
        assertThat(mesosEvent.getLeft()).isEqualTo(MesosEvent.TaskKillRequest);
        assertThat(mesosEvent.getRight()).isEqualTo(task.getId());
        return this;
    }

    public JobScenarioBuilder<E> triggerSchedulerLaunchEvent(int taskIdx, int resubmit) {
        Task task = findTaskInActiveState(taskIdx, resubmit);
        TaskState taskState = task.getStatus().getState();

        assertThat(taskState)
                .describedAs("Scheduler launch attempt for task in state: %s", taskState)
                .isEqualTo(TaskState.Accepted);

        Function<Task, Task> changeFunction = JobManagerUtil.newTaskLaunchConfigurationUpdater(
                "zone",
                vmService.buildLease(task.getId()),
                vmService.buildConsumeResult(task.getId()),
                vmService.buildAttributesMap(task.getId())
        );

        AtomicBoolean done = new AtomicBoolean();
        jobOperations.updateTaskAfterStore(task.getId(), changeFunction, Trigger.Scheduler, "Task launched by Fenzo")
                .subscribe(() -> done.set(true));
        advance();
        assertThat(done.get()).isTrue();

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

    public JobScenarioBuilder<E> triggerMesosFinishedEvent(int taskIdx, int resubmit, int errorCode) {
        return triggerMesosEvent(taskIdx, resubmit, TaskState.Finished, errorCode == 0 ? TaskStatus.REASON_NORMAL : TaskStatus.REASON_FAILED, errorCode);
    }

    private Task expectTaskEvent(int taskIdx, int resubmit, StoreEvent eventType) {
        Task lastInStore = (eventType == StoreEvent.TaskRemoved)
                ? jobStore.expectTaskInStoreArchive(jobId, taskIdx, resubmit)
                : jobStore.expectTaskInStore(jobId, taskIdx, resubmit);

        Pair<StoreEvent, ?> storeEventPair = storeEventsSubscriber.takeNext();
        assertThat(storeEventPair.getLeft()).isEqualTo(eventType);

        Task task = (Task) storeEventPair.getRight();
        assertThat(task.getId())
                .describedAs("Task version in store different from the task in the store event: %s != %s", lastInStore.getId(), task.getId())
                .isEqualTo(lastInStore.getId());

        return task;
    }

    private JobScenarioBuilder<E> triggerMesosEvent(int taskIdx, int resubmit, TaskState taskState, String reason, int errorCode) {
        Task task = jobStore.expectTaskInStore(jobId, taskIdx, resubmit);

        String reasonMessage;
        if (taskState == TaskState.Finished) {
            reasonMessage = errorCode == 0 ? "Completed successfully" : "Container terminated with an error " + errorCode;
        } else {
            reasonMessage = "Task changed state to " + taskState;
        }

        AtomicBoolean done = new AtomicBoolean();
        String data = taskState == TaskState.StartInitiated ? vmService.toString(vmService.buildExecutorDetails(task.getId())) : "";

        TaskStatus taskStatus = JobModel.newTaskStatus()
                .withState(taskState)
                .withReasonCode(reason)
                .withReasonMessage(reasonMessage)
                .withTimestamp(testScheduler.now())
                .build();

        Function<Task, Task> changeFunction = JobManagerUtil.newTaskStateUpdater(taskStatus, data);

        jobOperations.updateTask(task.getId(),
                changeFunction,
                Trigger.Mesos,
                String.format("Mesos callback taskStatus=%s, reason=%s (%s)", taskState, reason, reasonMessage)
        ).subscribe(() -> done.set(true));
        advance();
        assertThat(done.get()).isTrue();

        advance(); // As store update is done in second cycle always trigger it

        return this;
    }

    private <T> T autoAdvance(Supplier<T> action) {
        T result = action.get();
        if (result == null) {
            advance();
            return action.get();
        } else {
            return result;
        }
    }
}
