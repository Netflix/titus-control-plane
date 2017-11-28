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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.JobModel;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import io.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import io.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.jobmanager.service.JobManagerUtil;
import io.netflix.titus.master.jobmanager.service.event.JobChangeReconcilerEvent;
import io.netflix.titus.master.jobmanager.service.integration.scenario.StubbedJobStore.StoreEvent;
import io.netflix.titus.master.mesos.TitusExecutorDetails;
import io.netflix.titus.testkit.rx.ExtTestSubscriber;
import rx.schedulers.TestScheduler;

import static org.assertj.core.api.Assertions.assertThat;

public class JobScenarioBuilder<E extends JobDescriptor.JobDescriptorExt> {

    private final String jobId;
    private final ExtTestSubscriber<JobManagerEvent<?>> jobEventsSubscriber;
    private final ExtTestSubscriber<Pair<StoreEvent, ?>> storeEventsSubscriber;

    private final V3JobOperations jobOperations;
    private final StubbedSchedulingService schedulingService;
    private final StubbedJobStore jobStore;
    private final StubbedVirtualMachineMasterService vmService;
    private final TestScheduler testScheduler;

    private int nextTaskIdx;
    private Map<Integer, String> taskIdx2Id = new HashMap<>();


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
    }

    public JobScenarioBuilder<E> trigger() {
        testScheduler.triggerActions();
        return this;
    }

    public JobScenarioBuilder<E> advance() {
        testScheduler.advanceTimeBy(JobsScenarioBuilder.RECONCILER_ACTIVE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
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

    public JobScenarioBuilder<E> expectJobUpdateEvent() {
        JobManagerEvent<?> event = jobEventsSubscriber.takeNext();
        assertThat(event).describedAs("No job update event for job: %s", jobId).isNotNull();
        assertThat(event).isInstanceOf(JobUpdateEvent.class);

        JobUpdateEvent jobUpdateEvent = (JobUpdateEvent) event;
        assertThat(jobUpdateEvent.getCurrent().getId()).isEqualTo(jobId);

        return this;
    }

    public JobScenarioBuilder<E> expectTaskCreatedEvent() {
        JobManagerEvent<?> event = jobEventsSubscriber.takeNext();
        assertThat(event).isNotNull();
        assertThat(event).isInstanceOf(TaskUpdateEvent.class);
        return this;
    }

    public JobScenarioBuilder<E> expectedMesosChangedEvent(int taskIdx) {
        return expectTaskEvent(
                taskIdx,
                event -> event.getCurrent().getStatus().getReasonMessage().contains("mesos"),
                "Expected Mesos triggered task changed event"
        );
    }

    public JobScenarioBuilder<E> expectTaskUpdateEvent(int taskIdx, String expectedReason) {
        return expectTaskEvent(
                taskIdx,
                event -> event.getCurrent().getStatus().getReasonMessage().contains(expectedReason),
                "Expected task with " + expectedReason + " summary"
        );
    }

    public JobScenarioBuilder<E> expectScheduleRequest() {
        assertThat(schedulingService.getQueuableTasks()).isNotEmpty();
        return this;
    }

    private JobScenarioBuilder<E> expectTaskEvent(int taskIdx, Predicate<TaskUpdateEvent> predicate, String errorMessage) {
        JobManagerEvent<?> event = jobEventsSubscriber.takeNext();
        assertThat(event).isNotNull();
        assertThat(event).isInstanceOf(JobChangeReconcilerEvent.class);

        String taskId = taskIdx2Id.get(taskIdx);
        assertThat(taskId).describedAs("Unknown task id %s: ", taskId).isNotNull();

        assertThat(predicate.apply((TaskUpdateEvent) event)).describedAs("%s\nUnexpected event %s", errorMessage, event).isTrue();
        return this;
    }

    public JobScenarioBuilder<E> expectStoreJobUpdate() {
        assertThat(storeEventsSubscriber.takeNext().getLeft()).isEqualTo(StoreEvent.JobAdded);
        return this;
    }

    public JobScenarioBuilder<E> assertStoreTaskAddedEvent(Predicate<Task> check) {
        Pair<StoreEvent, ?> storeEventPair = storeEventsSubscriber.takeNext();
        assertThat(storeEventPair.getLeft()).isEqualTo(StoreEvent.TaskAdded);

        Task task = (Task) storeEventPair.getRight();
        taskIdx2Id.put(nextTaskIdx++, task.getId());

        Preconditions.checkState(check.apply(task), "Task store event with unexpected task state: %s", task);

        return this;
    }

    public JobScenarioBuilder<E> expectStoreTaskAdded() {
        return assertStoreTaskAddedEvent(task -> true);
    }

    public JobScenarioBuilder<E> expectStoreTaskArchived() {
        return this;
    }

    public JobScenarioBuilder<E> expectStoreTaskRemoved() {
        return this;
    }

    public JobScenarioBuilder<E> expectStoreJobRemoved() {
        return this;
    }

    public JobScenarioBuilder<E> triggerMesosLaunchEvent(int taskIdx) {
        return triggerMesosEvent(taskIdx, TaskState.Launched, "Task launched", null);
    }

    public JobScenarioBuilder<E> triggerMesosStartInitiatedEvent(int taskIdx) {
        String taskId = taskIdx2Id.get(taskIdx);
        assertThat(taskId).isNotNull();
        TitusExecutorDetails details = vmService.buildExecutorDetails();

        return triggerMesosEvent(taskIdx, TaskState.StartInitiated, "Starting container", vmService.toString(details));
    }

    public JobScenarioBuilder<E> triggerMesosStartedEvent(int taskIdx) {
        return triggerMesosEvent(taskIdx, TaskState.Started, "Task started", null);
    }

    public JobScenarioBuilder<E> triggerMesosFinishedEvent(int taskIdx) {
        return triggerMesosEvent(taskIdx, TaskState.Finished, TaskStatus.REASON_NORMAL, null);
    }

    private JobScenarioBuilder<E> triggerMesosEvent(int taskIdx, TaskState taskState, String reason, String data) {
        String taskId = taskIdx2Id.get(taskIdx);
        assertThat(taskId).isNotNull();

        TaskStatus taskStatus = JobModel.newTaskStatus()
                .withState(taskState)
                .withReasonCode(reason)
                .withReasonMessage("ScenarioBuilder")
                .withTimestamp(testScheduler.now())
                .build();

        AtomicBoolean done = new AtomicBoolean();
        jobOperations.updateTask(taskId, JobManagerUtil.newTaskStateUpdater(taskStatus, data), "Mesos -> " + taskState)
                .subscribe(() -> done.set(true));
        advance();
        assertThat(done.get()).isTrue();

        expectedMesosChangedEvent(taskIdx);

        return this;
    }
}
