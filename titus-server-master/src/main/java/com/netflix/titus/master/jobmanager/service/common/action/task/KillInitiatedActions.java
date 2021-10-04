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

package com.netflix.titus.master.jobmanager.service.common.action.task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;

import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.JobStatus;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.service.JobManagerConstants;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.jobmanager.store.JobStore;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.common.framework.reconciler.ChangeAction;
import com.netflix.titus.common.framework.reconciler.EntityHolder;
import com.netflix.titus.common.framework.reconciler.ModelActionHolder;
import com.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.master.jobmanager.service.JobServiceRuntime;
import com.netflix.titus.master.jobmanager.service.VersionSupplier;
import com.netflix.titus.master.jobmanager.service.VersionSuppliers;
import com.netflix.titus.master.jobmanager.service.common.action.JobEntityHolders;
import com.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import com.netflix.titus.master.jobmanager.service.common.action.TitusModelAction;
import com.netflix.titus.master.jobmanager.service.event.JobManagerReconcilerEvent;
import rx.Completable;
import rx.Observable;

/**
 * A collection of {@link ChangeAction}s for task termination.
 */
public class KillInitiatedActions {

    /**
     * Move job to {@link JobState#KillInitiated} state in reference, running and store models.
     */
    public static TitusChangeAction initiateJobKillAction(ReconciliationEngine<JobManagerReconcilerEvent> engine, JobStore titusStore,
                                                          VersionSupplier versionSupplier, String reason, CallMetadata callMetadata) {
        String reasonMessage = String.format("Changing job state to KillInitiated (reason:%s)", reason);
        return TitusChangeAction.newAction("initiateJobKillAction")
                .id(engine.getReferenceView().getId())
                .trigger(V3JobOperations.Trigger.API)
                .summary(reasonMessage)
                .callMetadata(callMetadata)
                .changeWithModelUpdates(self -> {
                    Job job = engine.getReferenceView().getEntity();
                    JobStatus newStatus = JobStatus.newBuilder()
                            .withState(JobState.KillInitiated)
                            .withReasonCode(TaskStatus.REASON_JOB_KILLED).withReasonMessage(reasonMessage)
                            .build();
                    Job jobWithKillInitiated = VersionSuppliers.nextVersion(JobFunctions.changeJobStatus(job, newStatus), versionSupplier);

                    TitusModelAction modelUpdateAction = TitusModelAction.newModelUpdate(self)
                            .jobMaybeUpdate(entityHolder -> Optional.of(entityHolder.setEntity(jobWithKillInitiated)));

                    return titusStore.updateJob(jobWithKillInitiated).andThen(Observable.just(ModelActionHolder.allModels(modelUpdateAction)));
                });
    }

    /**
     * Change a task to {@link TaskState#KillInitiated} state, store it, and send the kill command to the compute provider.
     * All models are updated when both operations complete.
     * This method is used for user initiated kill operations, so the store operation happens before response is sent back to the user.
     */
    public static ChangeAction userInitiateTaskKillAction(ReconciliationEngine<JobManagerReconcilerEvent> engine,
                                                          JobServiceRuntime executionContext,
                                                          JobStore jobStore,
                                                          VersionSupplier versionSupplier,
                                                          String taskId,
                                                          boolean shrink,
                                                          boolean preventMinSizeUpdate,
                                                          String reasonCode,
                                                          String reason,
                                                          TitusRuntime titusRuntime,
                                                          CallMetadata callMetadata) {
        return TitusChangeAction.newAction("userInitiateTaskKill")
                .id(taskId)
                .trigger(V3JobOperations.Trigger.API)
                .summary(reason)
                .callMetadata(callMetadata)
                .changeWithModelUpdates(self ->
                        JobEntityHolders.toTaskObservable(engine, taskId, titusRuntime).flatMap(task -> {
                            TaskState taskState = task.getStatus().getState();
                            if (taskState == TaskState.KillInitiated || taskState == TaskState.Finished) {
                                return Observable.just(Collections.<ModelActionHolder>emptyList());
                            }

                            if (shrink) {
                                Job<ServiceJobExt> job = engine.getReferenceView().getEntity();
                                Capacity capacity = job.getJobDescriptor().getExtensions().getCapacity();
                                if (preventMinSizeUpdate && capacity.getDesired() <= capacity.getMin()) {
                                    return Observable.<List<ModelActionHolder>>error(JobManagerException.terminateAndShrinkNotAllowed(job, task));
                                }
                            }

                            Task taskWithKillInitiated = VersionSuppliers.nextVersion(
                                    JobFunctions.changeTaskStatus(task, TaskState.KillInitiated, reasonCode, reason, titusRuntime.getClock()),
                                    versionSupplier);

                            Callable<List<ModelActionHolder>> modelUpdateActions = () -> JobEntityHolders.expectTask(engine, task.getId(), titusRuntime).map(current -> {
                                List<ModelActionHolder> updateActions = new ArrayList<>();

                                TitusModelAction stateUpdateAction = TitusModelAction.newModelUpdate(self).taskUpdate(taskWithKillInitiated);
                                updateActions.addAll(ModelActionHolder.allModels(stateUpdateAction));

                                if (shrink) {
                                    TitusModelAction shrinkAction = createShrinkAction(self, versionSupplier);
                                    updateActions.add(ModelActionHolder.reference(shrinkAction));
                                }
                                return updateActions;
                            }).orElse(Collections.emptyList());

                            return jobStore.updateTask(taskWithKillInitiated)
                                    .andThen(createKillAction(executionContext, task))
                                    .andThen(Observable.fromCallable(modelUpdateActions));
                        }));
    }

    /**
     * For an active task send kill command to the compute provider, and change its state to {@link TaskState#KillInitiated}.
     * This method is used for internal state reconciliation.
     */
    public static ChangeAction reconcilerInitiatedTaskKillInitiated(ReconciliationEngine<JobManagerReconcilerEvent> engine,
                                                                    Task task,
                                                                    JobServiceRuntime runtime,
                                                                    JobStore jobStore,
                                                                    VersionSupplier versionSupplier,
                                                                    String reasonCode,
                                                                    String reason,
                                                                    TitusRuntime titusRuntime) {
        return TitusChangeAction.newAction("reconcilerInitiatedTaskKill")
                .task(task)
                .trigger(V3JobOperations.Trigger.Reconciler)
                .summary(reason)
                .callMetadata(JobManagerConstants.RECONCILER_CALLMETADATA.toBuilder().withCallReason(reason).build())
                .changeWithModelUpdates(self ->
                        JobEntityHolders.toTaskObservable(engine, task.getId(), titusRuntime).flatMap(currentTask -> {
                            TaskState taskState = currentTask.getStatus().getState();
                            if (taskState == TaskState.Finished) {
                                return Observable.just(Collections.<ModelActionHolder>emptyList());
                            }

                            Task taskWithKillInitiated = VersionSuppliers.nextVersion(
                                    JobFunctions.changeTaskStatus(currentTask, TaskState.KillInitiated, reasonCode, reason, titusRuntime.getClock()),
                                    versionSupplier);
                            TitusModelAction taskUpdateAction = TitusModelAction.newModelUpdate(self).taskUpdate(taskWithKillInitiated);

                            // If already in KillInitiated state, do not store eagerly, just call the compute provider terminate again.
                            if (taskState == TaskState.KillInitiated) {
                                return createKillAction(runtime, currentTask).andThen(
                                        Observable.just(ModelActionHolder.referenceAndRunning(taskUpdateAction))
                                );
                            }

                            return jobStore.updateTask(taskWithKillInitiated)
                                    .andThen(createKillAction(runtime, currentTask))
                                    .andThen(Observable.fromCallable(() -> ModelActionHolder.allModels(taskUpdateAction)));
                        }));
    }

    /**
     * For all active tasks, send terminate command to the compute provider, and change their state to {@link TaskState#KillInitiated}.
     * This method is used for internal state reconciliation.
     */
    public static List<ChangeAction> reconcilerInitiatedAllTasksKillInitiated(ReconciliationEngine<JobManagerReconcilerEvent> engine,
                                                                              JobServiceRuntime runtime,
                                                                              JobStore jobStore,
                                                                              String reasonCode,
                                                                              String reason,
                                                                              int concurrencyLimit,
                                                                              VersionSupplier versionSupplier,
                                                                              TitusRuntime titusRuntime) {
        List<ChangeAction> result = new ArrayList<>();

        EntityHolder runningView = engine.getRunningView();

        Set<String> runningTaskIds = new HashSet<>();
        runningView.getChildren().forEach(taskHolder -> runningTaskIds.add(taskHolder.<Task>getEntity().getId()));

        // Immediately finish Accepted tasks, which are not yet in the running model.
        for (EntityHolder entityHolder : engine.getReferenceView().getChildren()) {
            if (result.size() >= concurrencyLimit) {
                return result;
            }

            Task task = entityHolder.getEntity();
            TaskState state = task.getStatus().getState();
            if (state == TaskState.Accepted && !runningTaskIds.contains(task.getId())) {
                result.add(BasicTaskActions.updateTaskAndWriteItToStore(
                        task.getId(),
                        engine,
                        taskRef -> JobFunctions.changeTaskStatus(taskRef, TaskState.Finished, reasonCode, reason, titusRuntime.getClock()),
                        jobStore,
                        V3JobOperations.Trigger.Reconciler,
                        reason,
                        versionSupplier,
                        titusRuntime,
                        JobManagerConstants.RECONCILER_CALLMETADATA.toBuilder().withCallReason(reason).build()
                ));
            }
        }

        // Move running tasks to KillInitiated state
        for (EntityHolder taskHolder : runningView.getChildren()) {
            if (result.size() >= concurrencyLimit) {
                return result;
            }

            Task task = taskHolder.getEntity();
            TaskState state = task.getStatus().getState();
            if (state != TaskState.KillInitiated && state != TaskState.Finished) {
                result.add(reconcilerInitiatedTaskKillInitiated(engine, task, runtime, jobStore, versionSupplier, reasonCode, reason, titusRuntime));
            }
        }

        return result;
    }

    private static Completable createKillAction(JobServiceRuntime executionContext, Task task) {
        return ReactorExt.toCompletable(executionContext.getComputeProvider().terminateTask(task));
    }

    private static TitusModelAction createShrinkAction(TitusChangeAction.Builder changeActionBuilder, VersionSupplier versionSupplier) {
        return TitusModelAction.newModelUpdate(changeActionBuilder)
                .summary("Shrinking job as a result of terminate and shrink request")
                .jobUpdate(jobHolder -> {
                    Job<ServiceJobExt> serviceJob = jobHolder.getEntity();
                    ServiceJobExt oldExt = serviceJob.getJobDescriptor().getExtensions();

                    Capacity oldCapacity = oldExt.getCapacity();
                    // A job of size 0 may still have some tasks running just after job scale-down request.
                    // We must make sure that we do not decrement the job size if it is already 0.
                    int newDesired = Math.max(0, oldCapacity.getDesired() - 1);
                    Capacity newCapacity = oldCapacity.toBuilder()
                            .withMin(Math.min(oldCapacity.getMin(), newDesired))
                            .withDesired(newDesired)
                            .build();

                    Job<ServiceJobExt> newJob = serviceJob.toBuilder()
                            .withJobDescriptor(
                                    serviceJob.getJobDescriptor().toBuilder()
                                            .withExtensions(oldExt.toBuilder().withCapacity(newCapacity).build())
                                            .build())
                            .build();
                    newJob = VersionSuppliers.nextVersion(newJob, versionSupplier);
                    return jobHolder.setEntity(newJob);
                });
    }
}
