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

package io.netflix.titus.master.jobmanager.service.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.JobState;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.api.jobmanager.store.JobStore;
import io.netflix.titus.common.framework.reconciler.ChangeAction;
import io.netflix.titus.common.framework.reconciler.EntityHolder;
import io.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import io.netflix.titus.common.util.time.Clock;
import io.netflix.titus.master.VirtualMachineMasterService;
import io.netflix.titus.master.jobmanager.service.JobManagerConfiguration;
import io.netflix.titus.master.jobmanager.service.common.action.task.BasicJobActions;
import io.netflix.titus.master.jobmanager.service.common.action.task.BasicTaskActions;
import io.netflix.titus.master.jobmanager.service.common.action.task.KillInitiatedActions;
import io.netflix.titus.master.jobmanager.service.common.action.task.TaskTimeoutChangeActions;
import io.netflix.titus.master.jobmanager.service.event.JobManagerReconcilerEvent;

/**
 * Collection of functions useful for batch and service difference resolvers.
 */
public class DifferenceResolverUtils {

    public static boolean isDone(Job<?> job, Task task) {
        return task.getStatus().getState() == TaskState.Finished && !shouldRetry(job, task);
    }

    public static boolean allDone(EntityHolder rootHolder) {
        return rootHolder.getChildren().stream().allMatch(taskHolder -> isDone(rootHolder.getEntity(), taskHolder.getEntity()));
    }

    public static boolean shouldRetry(Job<?> job, Task task) {
        TaskStatus status = task.getStatus();
        if (status.getState() != TaskState.Finished) {
            return false;
        }
        if (hasReachedRetryLimit(job, task)) {
            return false;
        }
        if (!isBatch(job)) {
            return true;
        }
        String reasonCode = status.getReasonCode();
        return !TaskStatus.REASON_NORMAL.equals(reasonCode) && !TaskStatus.REASON_TASK_KILLED.equals(reasonCode);
    }

    public static boolean hasReachedRetryLimit(Job<?> refJob, Task task) {
        int retryLimit = apply(refJob, BatchJobExt::getRetryPolicy, ServiceJobExt::getRetryPolicy).getRetries();
        return task.getStatus().getState() == TaskState.Finished && task.getResubmitNumber() >= retryLimit;
    }

    public static boolean hasJobState(EntityHolder root, JobState state) {
        Job job = root.getEntity();
        return job.getStatus().getState() == state;
    }

    public static boolean areEquivalent(EntityHolder storeTask, EntityHolder referenceTask) {
        return storeTask.getEntity().equals(referenceTask.getEntity());
    }

    public static boolean isBatch(Job<?> job) {
        return job.getJobDescriptor().getExtensions() instanceof BatchJobExt;
    }

    public static boolean isTerminating(Task task) {
        TaskState state = task.getStatus().getState();
        return state == TaskState.KillInitiated || state == TaskState.Finished;
    }

    public static <T> T apply(Job<?> job, Function<BatchJobExt, T> batch, Function<ServiceJobExt, T> service) {
        JobDescriptor.JobDescriptorExt ext = job.getJobDescriptor().getExtensions();
        if (ext instanceof BatchJobExt) {
            return batch.apply((BatchJobExt) ext);
        }
        return service.apply((ServiceJobExt) ext);
    }

    /**
     * Find all tasks that are stuck in a specific state
     */
    public static List<ChangeAction> findTaskStateTimeouts(ReconciliationEngine<JobManagerReconcilerEvent> engine,
                                                           JobView runningJobView,
                                                           JobManagerConfiguration configuration,
                                                           Clock clock,
                                                           VirtualMachineMasterService vmService) {
        List<ChangeAction> actions = new ArrayList<>();
        runningJobView.getJobHolder().getChildren().forEach(taskHolder -> {
            Task task = taskHolder.getEntity();
            TaskState taskState = task.getStatus().getState();
            TaskTimeoutChangeActions.TimeoutStatus timeoutStatus = TaskTimeoutChangeActions.getTimeoutStatus(taskHolder, clock);
            switch (timeoutStatus) {
                case Ignore:
                case Pending:
                    break;
                case NotSet:
                    long timeoutMs = -1;
                    switch (taskState) {
                        case Launched:
                            timeoutMs = configuration.getTaskInLaunchedStateTimeoutMs();
                            break;
                        case StartInitiated:
                            timeoutMs = isBatch(runningJobView.getJob())
                                    ? configuration.getBatchTaskInStartInitiatedStateTimeoutMs()
                                    : configuration.getServiceTaskInStartInitiatedStateTimeoutMs();
                            break;
                        case KillInitiated:
                            timeoutMs = configuration.getTaskInKillInitiatedStateTimeoutMs();
                            break;
                    }
                    if (timeoutMs > 0) {
                        actions.add(TaskTimeoutChangeActions.setTimeout(taskHolder.getId(), task.getStatus().getState(), clock.wallTime() + timeoutMs));
                    }
                    break;
                case TimedOut:
                    if (task.getStatus().getState() == TaskState.KillInitiated) {
                        int attempts = TaskTimeoutChangeActions.getKillInitiatedAttempts(taskHolder) + 1;
                        if (attempts >= configuration.getTaskKillAttempts()) {
                            actions.add(
                                    BasicTaskActions.updateTaskInRunningModel(task.getId(),
                                            V3JobOperations.Trigger.Reconciler,
                                            engine,
                                            taskParam -> taskParam.toBuilder()
                                                    .withStatus(taskParam.getStatus().toBuilder()
                                                            .withState(TaskState.Finished)
                                                            .withReasonCode(TaskStatus.REASON_STUCK_IN_STATE)
                                                            .withReasonMessage("stuck in " + taskState + "state")
                                                            .build()
                                                    )
                                                    .build(),
                                            "TimedOut in KillInitiated state"
                                    )
                            );
                        } else {
                            actions.add(TaskTimeoutChangeActions.incrementTaskKillAttempt(task.getId(), clock.wallTime() + configuration.getTaskInKillInitiatedStateTimeoutMs()));
                            actions.add(KillInitiatedActions.applyKillInitiated(engine, task, vmService, TaskStatus.REASON_STUCK_IN_STATE, "Another kill attempt (" + (attempts + 1) + ')'));
                        }
                    } else {
                        actions.add(KillInitiatedActions.applyKillInitiated(engine, task, vmService, TaskStatus.REASON_STUCK_IN_STATE, "Task stuck in " + taskState + " state"));
                    }
                    break;
            }
        });
        return actions;
    }

    public static List<ChangeAction> removeCompletedJob(EntityHolder referenceModel, EntityHolder storeModel, JobStore titusStore) {
        if (!hasJobState(referenceModel, JobState.Finished)) {
            if (allDone(storeModel)) {
                return Collections.singletonList(BasicJobActions.completeJob(referenceModel.getId()));
            }
        } else {
            if (!BasicJobActions.isClosed(referenceModel)) {
                return Collections.singletonList(BasicJobActions.removeJobFromStore(referenceModel.getEntity(), titusStore));
            }

        }
        return Collections.emptyList();
    }

    public static class JobView<EXT extends JobDescriptor.JobDescriptorExt, TASK extends Task> {

        private final Job<EXT> job;
        private final EntityHolder jobHolder;
        private final List<TASK> tasks;
        private final int requiredSize;

        public JobView(EntityHolder jobHolder) {
            this.job = jobHolder.getEntity();
            this.jobHolder = jobHolder;
            this.requiredSize = apply(job, BatchJobExt::getSize, service -> service.getCapacity().getDesired());
            this.tasks = jobHolder.getChildren().stream().map(h -> (TASK) h.getEntity()).collect(Collectors.toList());
        }

        public EntityHolder getJobHolder() {
            return jobHolder;
        }

        public Job<EXT> getJob() {
            return job;
        }

        public List<TASK> getTasks() {
            return tasks;
        }

        public TASK getTaskById(String refTaskId) {
            for (TASK task : tasks) {
                if (task.getId().equals(refTaskId)) {
                    return task;
                }
            }
            return null;
        }

        public int getRequiredSize() {
            return requiredSize;
        }
    }
}
