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
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.jobmanager.store.JobStore;
import com.netflix.titus.common.framework.reconciler.ChangeAction;
import com.netflix.titus.common.framework.reconciler.ModelActionHolder;
import com.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import com.netflix.titus.master.VirtualMachineMasterService;
import com.netflix.titus.master.jobmanager.service.common.action.JobEntityHolders;
import com.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import com.netflix.titus.master.jobmanager.service.common.action.TitusModelAction;
import com.netflix.titus.master.jobmanager.service.event.JobManagerReconcilerEvent;
import rx.Completable;
import rx.Observable;
import rx.functions.Action0;

/**
 * A collection of {@link ChangeAction}s for task termination.
 */
public class KillInitiatedActions {

    /**
     * Move job to {@link JobState#KillInitiated} state in reference, running and store models.
     */
    public static TitusChangeAction initiateJobKillAction(ReconciliationEngine<JobManagerReconcilerEvent> engine, JobStore titusStore) {
        return TitusChangeAction.newAction("initiateJobKillAction")
                .id(engine.getReferenceView().getId())
                .trigger(V3JobOperations.Trigger.API)
                .summary("Changing job state to KillInitiated")
                .changeWithModelUpdates(self -> {
                    Job job = engine.getReferenceView().getEntity();
                    JobStatus newStatus = JobStatus.newBuilder()
                            .withState(JobState.KillInitiated)
                            .withReasonCode(TaskStatus.REASON_JOB_KILLED).withReasonMessage("External job termination request")
                            .build();
                    Job jobWithKillInitiated = JobFunctions.changeJobStatus(job, newStatus);

                    TitusModelAction modelUpdateAction = TitusModelAction.newModelUpdate(self)
                            .jobMaybeUpdate(entityHolder -> Optional.of(entityHolder.setEntity(jobWithKillInitiated)));

                    return titusStore.updateJob(jobWithKillInitiated).andThen(Observable.just(ModelActionHolder.allModels(modelUpdateAction)));
                });
    }

    /**
     * Change a task to {@link TaskState#KillInitiated} state, store it, and send the kill command to Mesos.
     * All models are updated when both operations complete.
     * This method is used for user initiated kill operations, so the store operation happens before response is sent back to the user.
     */
    public static ChangeAction userInitiateTaskKillAction(ReconciliationEngine<JobManagerReconcilerEvent> engine,
                                                          VirtualMachineMasterService vmService,
                                                          JobStore jobStore,
                                                          String taskId,
                                                          boolean shrink,
                                                          String reasonCode,
                                                          String reason) {
        return TitusChangeAction.newAction("userInitiateTaskKill")
                .id(taskId)
                .trigger(V3JobOperations.Trigger.API)
                .summary(reason)
                .changeWithModelUpdates(self ->
                        JobEntityHolders.toTaskObservable(engine, taskId).flatMap(task -> {
                            TaskState taskState = task.getStatus().getState();
                            if (taskState == TaskState.KillInitiated || taskState == TaskState.Finished) {
                                return Observable.just(Collections.<ModelActionHolder>emptyList());
                            }
                            Task taskWithKillInitiated = JobFunctions.changeTaskStatus(task, TaskState.KillInitiated, reasonCode, reason);

                            Action0 killAction = () -> vmService.killTask(taskId);
                            Callable<List<ModelActionHolder>> modelUpdateActions = () -> JobEntityHolders.expectTask(engine, task.getId()).map(current -> {
                                List<ModelActionHolder> updateActions = new ArrayList<>();

                                TitusModelAction stateUpdateAction = TitusModelAction.newModelUpdate(self).taskUpdate(taskWithKillInitiated);
                                updateActions.addAll(ModelActionHolder.allModels(stateUpdateAction));

                                if (shrink) {
                                    TitusModelAction shrinkAction = createShrinkAction(self);
                                    updateActions.add(ModelActionHolder.reference(shrinkAction));
                                }
                                return updateActions;
                            }).orElse(Collections.emptyList());

                            return jobStore.updateTask(taskWithKillInitiated)
                                    .andThen(Completable.fromAction(killAction))
                                    .andThen(Observable.fromCallable(modelUpdateActions));
                        }));
    }

    /**
     * For an active task send kill command to Mesos, and change its state to {@link TaskState#KillInitiated}.
     * This method is used for internal state reconciliation.
     */
    public static ChangeAction reconcilerInitiatedTaskKillInitiated(ReconciliationEngine<JobManagerReconcilerEvent> engine,
                                                                    Task task,
                                                                    VirtualMachineMasterService vmService,
                                                                    JobStore jobStore,
                                                                    String reasonCode,
                                                                    String reason) {
        return TitusChangeAction.newAction("reconcilerInitiatedTaskKill")
                .task(task)
                .trigger(V3JobOperations.Trigger.Reconciler)
                .summary(reason)
                .changeWithModelUpdates(self ->
                        JobEntityHolders.toTaskObservable(engine, task.getId()).flatMap(currentTask -> {
                            TaskState taskState = currentTask.getStatus().getState();
                            if (taskState == TaskState.Finished) {
                                return Observable.just(Collections.<ModelActionHolder>emptyList());
                            }

                            Task taskWithKillInitiated = JobFunctions.changeTaskStatus(currentTask, TaskState.KillInitiated, reasonCode, reason);
                            TitusModelAction taskUpdateAction = TitusModelAction.newModelUpdate(self).taskUpdate(taskWithKillInitiated);

                            // If already in KillInitiated state, do not store eagerly, just call Mesos kill again.
                            if (taskState == TaskState.KillInitiated) {
                                vmService.killTask(currentTask.getId());
                                return Observable.just(ModelActionHolder.referenceAndRunning(taskUpdateAction));
                            }

                            return jobStore.updateTask(taskWithKillInitiated)
                                    .andThen(Completable.fromAction(() -> vmService.killTask(currentTask.getId())))
                                    .andThen(Observable.fromCallable(() -> ModelActionHolder.allModels(taskUpdateAction)));
                        }));
    }

    /**
     * For all active tasks, send kill command to Mesos, and change their state to {@link TaskState#KillInitiated}.
     * This method is used for internal state reconciliation.
     */
    public static List<ChangeAction> reconcilerInitiatedAllTasksKillInitiated(ReconciliationEngine<JobManagerReconcilerEvent> engine,
                                                                              VirtualMachineMasterService vmService,
                                                                              JobStore jobStore,
                                                                              String reasonCode,
                                                                              String reason) {
        List<ChangeAction> result = new ArrayList<>();

        // Move running tasks to KillInitiated state
        Set<String> runningTaskIds = new HashSet<>();
        engine.getRunningView().getChildren().forEach(taskHolder -> {
            Task task = taskHolder.getEntity();
            runningTaskIds.add(task.getId());

            TaskState state = task.getStatus().getState();
            if (state != TaskState.KillInitiated && state != TaskState.Finished) {
                result.add(reconcilerInitiatedTaskKillInitiated(engine, task, vmService, jobStore, reasonCode, reason));
            }
        });

        // Immediately finish Accepted tasks, which are not yet in the running model.
        engine.getReferenceView().getChildren().forEach(taskHolder -> {
            Task task = taskHolder.getEntity();
            TaskState state = task.getStatus().getState();
            if (state == TaskState.Accepted && !runningTaskIds.contains(task.getId())) {
                result.add(BasicTaskActions.updateTaskAndWriteItToStore(
                        task.getId(),
                        engine,
                        taskRef -> JobFunctions.changeTaskStatus(taskRef, TaskState.Finished, reasonCode, reason),
                        jobStore,
                        V3JobOperations.Trigger.Reconciler,
                        reason
                ));
            }
        });

        return result;
    }

    private static TitusModelAction createShrinkAction(TitusChangeAction.Builder changeActionBuilder) {
        return TitusModelAction.newModelUpdate(changeActionBuilder)
                .summary("Shrinking job as a result of terminate and shrink request")
                .jobUpdate(jobHolder -> {
                    Job<ServiceJobExt> serviceJob = jobHolder.getEntity();
                    ServiceJobExt oldExt = serviceJob.getJobDescriptor().getExtensions();

                    Capacity oldCapacity = oldExt.getCapacity();
                    int newDesired = oldCapacity.getDesired() - 1;
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
                    return jobHolder.setEntity(newJob);
                });
    }
}
