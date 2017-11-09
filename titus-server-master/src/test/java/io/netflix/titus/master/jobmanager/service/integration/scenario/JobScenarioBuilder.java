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
import java.util.function.Consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import io.netflix.titus.api.jobmanager.model.event.JobEvent;
import io.netflix.titus.api.jobmanager.model.event.JobManagerEvent;
import io.netflix.titus.api.jobmanager.model.event.JobManagerEvent.Trigger;
import io.netflix.titus.api.jobmanager.model.event.TaskEvent;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.JobModel;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.jobmanager.service.DefaultV3JobOperations;
import io.netflix.titus.master.jobmanager.service.JobManagerConfiguration;
import io.netflix.titus.master.jobmanager.service.JobManagerUtil;
import io.netflix.titus.master.jobmanager.service.batch.BatchDifferenceResolver;
import io.netflix.titus.master.jobmanager.service.integration.scenario.StubbedJobStore.StoreEvent;
import io.netflix.titus.master.jobmanager.service.service.ServiceDifferenceResolver;
import io.netflix.titus.master.service.management.ApplicationSlaManagementService;
import io.netflix.titus.testkit.rx.ExtTestSubscriber;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JobScenarioBuilder<E extends JobDescriptor.JobDescriptorExt> {

    public static final long RECONCILER_ACTIVE_TIMEOUT_MS = 50L;
    public static final long RECONCILER_IDLE_TIMEOUT_MS = 50;

    private enum RunningState {NotActivated, Activated, JobCreated, JobFinished}

    private final TestScheduler testScheduler = Schedulers.test();

    private final JobManagerConfiguration configuration = mock(JobManagerConfiguration.class);
    private final ApplicationSlaManagementService capacityGroupService = new StubbedApplicationSlaManagementService();
    private final StubbedSchedulingService schedulingService = new StubbedSchedulingService();
    private final StubbedVirtualMachineMasterService vmService = new StubbedVirtualMachineMasterService();
    private final StubbedJobStore jobStore = new StubbedJobStore();

    private final DefaultV3JobOperations jobOperations;
    private final JobDescriptor<E> jobDescriptor;

    private RunningState runningState = RunningState.NotActivated;
    private String jobId;
    private int nextTaskIdx;
    private Map<Integer, String> taskIdx2Id = new HashMap<>();

    private final ExtTestSubscriber<Pair<StoreEvent, ?>> storeEvents = new ExtTestSubscriber<>();
    private final ExtTestSubscriber<JobManagerEvent> jobEvents = new ExtTestSubscriber<>();

    public JobScenarioBuilder(JobDescriptor<E> jobDescriptor) {
        when(configuration.getReconcilerActiveTimeoutMs()).thenReturn(RECONCILER_ACTIVE_TIMEOUT_MS);
        when(configuration.getReconcilerIdleTimeoutMs()).thenReturn(RECONCILER_IDLE_TIMEOUT_MS);

        this.jobDescriptor = jobDescriptor;

        jobStore.events().subscribe(storeEvents);

        BatchDifferenceResolver batchDifferenceResolver = new BatchDifferenceResolver(
                configuration,
                capacityGroupService,
                schedulingService,
                vmService,
                jobStore
        );
        ServiceDifferenceResolver serviceDifferenceResolver = new ServiceDifferenceResolver(
                configuration,
                capacityGroupService,
                schedulingService,
                vmService,
                jobStore
        );
        this.jobOperations = new DefaultV3JobOperations(
                configuration,
                batchDifferenceResolver,
                serviceDifferenceResolver,
                jobStore,
                schedulingService,
                vmService,
                capacityGroupService,
                testScheduler
        );
    }

    public JobScenarioBuilder<E> trigger() {
        testScheduler.triggerActions();
        return this;
    }

    public JobScenarioBuilder<E> advance() {
        testScheduler.advanceTimeBy(RECONCILER_ACTIVE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        return this;
    }

    public JobScenarioBuilder<?> ignoreAvailableEvents() {
        while (jobEvents.takeNext() != null) {
        }
        while (storeEvents.takeNext() != null) {
        }
        return this;
    }

    public JobScenarioBuilder<E> activate() {
        ensureInState(RunningState.NotActivated);
        jobOperations.enterActiveMode();

        runningState = RunningState.Activated;
        return this;
    }

    public JobScenarioBuilder<E> submit() {
        ensureInState(RunningState.Activated);

        jobOperations.createJob(jobDescriptor).flatMap(jobId -> {
            JobScenarioBuilder.this.jobId = jobId;
            return jobOperations.observeJob(jobId);
        }).subscribe(jobEvents);

        runningState = RunningState.JobCreated;
        return this;
    }

    public JobScenarioBuilder<E> expectStoreJobUpdate() {
        assertThat(storeEvents.takeNext().getLeft()).isEqualTo(StoreEvent.JobAdded);
        return this;
    }

    public JobScenarioBuilder<E> expectStoreTaskAdded() {
        return expectStoreTaskAdded(null);
    }

    public JobScenarioBuilder<E> expectStoreTaskAdded(Consumer<Task> check) {
        Pair<StoreEvent, ?> storeEventPair = storeEvents.takeNext();
        assertThat(storeEventPair.getLeft()).isEqualTo(StoreEvent.TaskAdded);

        Task task = (Task) storeEventPair.getRight();
        taskIdx2Id.put(nextTaskIdx++, task.getId());

        if (check != null) {
            check.accept(task);
        }

        return this;
    }

    public JobScenarioBuilder<E> expectJobUpdateEvent() {
        JobManagerEvent event = jobEvents.takeNext();
        assertThat(event).isNotNull();
        assertThat(event).isInstanceOf(JobEvent.class);

        JobEvent jobEvent = (JobEvent) event;
        assertThat(jobEvent.getId()).isEqualTo(jobId);

        return this;
    }

    public JobScenarioBuilder<E> expectTaskCreatedEvent() {
        JobManagerEvent event = jobEvents.takeNext();
        assertThat(event).isNotNull();
        assertThat(event).isInstanceOf(TaskEvent.class);

        TaskEvent taskEvent = (TaskEvent) event;
        assertThat(taskEvent.getSummary()).contains("Creating new task");

        return this;
    }

    public JobScenarioBuilder<E> expectTaskUpdateEvent(int taskIdx, String message) {
        expectTaskEvent(taskIdx, event -> event.getSummary().contains(message));

        return this;
    }

    public JobScenarioBuilder<E> expectScheduleRequest() {
        assertThat(schedulingService.getQueuableTasks()).isNotEmpty();
        return this;
    }

    private void expectTaskEvent(int taskIdx, Predicate<TaskEvent> predicate) {
        JobManagerEvent event = jobEvents.takeNext();
        assertThat(event).isNotNull();
        assertThat(event).isInstanceOf(TaskEvent.class);

        String taskId = taskIdx2Id.get(taskIdx);
        assertThat(taskId).describedAs("Unknown task id %s: ", taskId).isNotNull();

        assertThat(predicate.apply((TaskEvent) event)).describedAs("Unexpected event %s", event).isTrue();
    }

    public JobScenarioBuilder<E> triggerMesosEvent(int taskIdx, TaskState taskState) {
        return triggerMesosEvent(taskIdx, taskState, "ScenarioBuilder");
    }

    public JobScenarioBuilder<E> triggerMesosEvent(int taskIdx, TaskState taskState, String reason) {
        return triggerMesosEvent(taskIdx, taskState, reason, null);
    }

    public JobScenarioBuilder<E> triggerMesosEvent(int taskIdx, TaskState taskState, String reason, String data) {
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

        expectTaskEvent(taskIdx, event -> event.getTrigger() == Trigger.Mesos);

        return this;
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

    private void ensureInState(RunningState expectedState) {
        Preconditions.checkState(expectedState == runningState, "Expected state (%s) != actual state (%s)", expectedState, runningState);
    }
}
