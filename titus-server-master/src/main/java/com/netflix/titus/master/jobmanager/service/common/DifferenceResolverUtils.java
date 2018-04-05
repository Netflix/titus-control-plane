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

package com.netflix.titus.master.jobmanager.service.common;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.jobmanager.store.JobStore;
import com.netflix.titus.common.framework.reconciler.ChangeAction;
import com.netflix.titus.common.framework.reconciler.EntityHolder;
import com.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.master.VirtualMachineMasterService;
import com.netflix.titus.master.jobmanager.service.JobManagerConfiguration;
import com.netflix.titus.master.jobmanager.service.common.action.task.BasicTaskActions;
import com.netflix.titus.master.jobmanager.service.common.action.task.KillInitiatedActions;
import com.netflix.titus.master.jobmanager.service.common.action.task.TaskTimeoutChangeActions;
import com.netflix.titus.master.jobmanager.service.event.JobManagerReconcilerEvent;

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
        TaskStatus taskStatus = task.getStatus();
        if (taskStatus.getState() != TaskState.Finished || job.getStatus().getState() != JobState.Accepted) {
            return false;
        }
        if (hasReachedRetryLimit(job, task)) {
            return false;
        }
        if (!isBatch(job)) {
            return true;
        }
        String reasonCode = taskStatus.getReasonCode();
        return !TaskStatus.REASON_NORMAL.equals(reasonCode);
    }

    public static boolean hasReachedRetryLimit(Job<?> refJob, Task task) {
        int retryLimit = apply(refJob, BatchJobExt::getRetryPolicy, ServiceJobExt::getRetryPolicy).getRetries();
        if (task.getStatus().getState() != TaskState.Finished || TaskStatus.isSystemError(task.getStatus())) {
            return false;
        }
        int userRetries = task.getResubmitNumber() - task.getSystemResubmitNumber();
        return userRetries >= retryLimit;
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
                                                           VirtualMachineMasterService vmService,
                                                           JobStore jobStore) {
        List<ChangeAction> actions = new ArrayList<>();
        runningJobView.getJobHolder().getChildren().forEach(taskHolder -> {
            Task task = taskHolder.getEntity();
            TaskState taskState = task.getStatus().getState();

            if (JobFunctions.isBatchJob(runningJobView.getJob()) && taskState == TaskState.Started) {
                Job<BatchJobExt> batchJob = runningJobView.getJob();

                // We expect runtime limit to be always set, so this is just extra safety measure.
                long runtimeLimitMs = Math.max(60_000, batchJob.getJobDescriptor().getExtensions().getRuntimeLimitMs());

                long deadline = task.getStatus().getTimestamp() + runtimeLimitMs;
                if (deadline < clock.wallTime()) {
                    actions.add(KillInitiatedActions.reconcilerInitiatedTaskKillInitiated(engine, task, vmService, jobStore,
                            TaskStatus.REASON_RUNTIME_LIMIT_EXCEEDED, "Task running too long (runtimeLimit=" + runtimeLimitMs + "ms)")
                    );
                }
                return;
            }

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
                        actions.add(TaskTimeoutChangeActions.setTimeout(taskHolder.getId(), task.getStatus().getState(), timeoutMs, clock));
                    }
                    break;
                case TimedOut:
                    if (task.getStatus().getState() == TaskState.KillInitiated) {
                        int attempts = TaskTimeoutChangeActions.getKillInitiatedAttempts(taskHolder) + 1;
                        if (attempts >= configuration.getTaskKillAttempts()) {
                            actions.add(
                                    BasicTaskActions.updateTaskInRunningModel(task.getId(),
                                            V3JobOperations.Trigger.Reconciler,
                                            configuration,
                                            engine,
                                            taskParam -> Optional.of(taskParam.toBuilder()
                                                    .withStatus(taskParam.getStatus().toBuilder()
                                                            .withState(TaskState.Finished)
                                                            .withReasonCode(TaskStatus.REASON_STUCK_IN_KILLING_STATE)
                                                            .withReasonMessage("stuck in " + taskState + "state")
                                                            .build()
                                                    )
                                                    .build()
                                            ),
                                            "TimedOut in KillInitiated state",
                                            clock
                                    )
                            );
                        } else {
                            actions.add(TaskTimeoutChangeActions.incrementTaskKillAttempt(task.getId(), configuration.getTaskInKillInitiatedStateTimeoutMs(), clock));
                            actions.add(KillInitiatedActions.reconcilerInitiatedTaskKillInitiated(engine, task, vmService, jobStore, TaskStatus.REASON_STUCK_IN_KILLING_STATE, "Another kill attempt (" + (attempts + 1) + ')'));
                        }
                    } else {
                        actions.add(KillInitiatedActions.reconcilerInitiatedTaskKillInitiated(engine, task, vmService, jobStore, TaskStatus.REASON_STUCK_IN_STATE, "Task stuck in " + taskState + " state"));
                    }
                    break;
            }
        });
        return actions;
    }

    public static int countActiveNotStartedTasks(EntityHolder refJobHolder, EntityHolder runningJobHolder) {
        Set<String> pendingTaskIds = new HashSet<>();

        Consumer<EntityHolder> countingFun = jobHolder -> jobHolder.getChildren().forEach(taskHolder -> {
            TaskState state = ((Task) taskHolder.getEntity()).getStatus().getState();
            if (state != TaskState.Started && state != TaskState.Finished) {
                pendingTaskIds.add(taskHolder.getId());
            }
        });
        countingFun.accept(refJobHolder);
        countingFun.accept(runningJobHolder);

        return pendingTaskIds.size();
    }

    public static class JobView<EXT extends JobDescriptor.JobDescriptorExt, TASK extends Task> {

        private final Job<EXT> job;
        private final EntityHolder jobHolder;
        private final List<TASK> tasks;
        private final int requiredSize;

        @SuppressWarnings("unchecked")
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
