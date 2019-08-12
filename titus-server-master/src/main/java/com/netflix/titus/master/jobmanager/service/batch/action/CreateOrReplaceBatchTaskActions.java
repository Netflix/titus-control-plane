/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.master.jobmanager.service.batch.action;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.service.JobManagerConstants;
import com.netflix.titus.api.jobmanager.service.V3JobOperations.Trigger;
import com.netflix.titus.api.jobmanager.store.JobStore;
import com.netflix.titus.common.framework.reconciler.EntityHolder;
import com.netflix.titus.common.framework.reconciler.ModelActionHolder;
import com.netflix.titus.common.util.retry.Retryer;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.master.jobmanager.service.JobManagerConfiguration;
import com.netflix.titus.master.jobmanager.service.common.action.TaskRetryers;
import com.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import com.netflix.titus.master.jobmanager.service.common.action.TitusModelAction;
import rx.Observable;

/**
 * Create a new task or replace a completed task, and persist it into the store. Update reference/store models.
 */
public class CreateOrReplaceBatchTaskActions {

    public static TitusChangeAction createOrReplaceTaskAction(JobManagerConfiguration configuration,
                                                              JobStore jobStore,
                                                              EntityHolder jobHolder,
                                                              int index,
                                                              Clock clock,
                                                              Map<String, String> taskContext) {
        return jobHolder.getChildren().stream()
                .filter(taskHolder -> {
                    BatchJobTask task = taskHolder.getEntity();
                    return task.getIndex() == index;
                })
                .findFirst()
                .map(taskHolder -> createResubmittedTaskChangeAction(jobHolder, taskHolder, configuration, jobStore, clock, taskContext))
                .orElseGet(() -> createOriginalTaskChangeAction(jobHolder.getEntity(), index, jobStore, clock, taskContext));
    }

    private static TitusChangeAction createOriginalTaskChangeAction(Job<BatchJobExt> job, int index, JobStore jobStore, Clock clock, Map<String, String> taskContext) {
        Retryer newRetryer = JobFunctions.retryer(job);
        BatchJobTask newTask = createNewBatchTask(job, index, clock.wallTime(), taskContext);

        return TitusChangeAction.newAction("createOrReplaceTask")
                .id(newTask.getId())
                .trigger(Trigger.Reconciler)
                .summary(
                        String.format("Creating new task at index %d in DB store: %s", newTask.getIndex(), newTask.getId())
                )
                .changeWithModelUpdates(self -> jobStore.storeTask(newTask).andThen(Observable.just(createNewTaskModelAction(self, newTask, newRetryer))));
    }

    private static TitusChangeAction createResubmittedTaskChangeAction(EntityHolder jobHolder,
                                                                       EntityHolder taskHolder,
                                                                       JobManagerConfiguration configuration,
                                                                       JobStore jobStore,
                                                                       Clock clock,
                                                                       Map<String, String> taskContext) {
        BatchJobTask oldTask = taskHolder.getEntity();
        long timeInStartedState = JobFunctions.getTimeInState(oldTask, TaskState.Started, clock).orElse(0L);
        Retryer nextTaskRetryer = timeInStartedState >= configuration.getTaskRetryerResetTimeMs()
                ? JobFunctions.retryer(jobHolder.getEntity())
                : TaskRetryers.getNextTaskRetryer(jobHolder.getEntity(), taskHolder);
        BatchJobTask newTask = createBatchTaskReplacement(oldTask, clock, taskContext);

        String summary = String.format(
                "Replacing task at index %d (resubmit=%d) in DB store: old=%s, new=%s",
                oldTask.getIndex(),
                newTask.getResubmitNumber(),
                oldTask.getId(),
                newTask.getId()
        );

        return TitusChangeAction.newAction("createOrReplaceTask")
                .id(newTask.getId())
                .trigger(Trigger.Reconciler)
                .summary(summary)
                .changeWithModelUpdates(self -> jobStore.replaceTask(oldTask, newTask).andThen(Observable.just(createTaskResubmitModelActions(self, oldTask, newTask, nextTaskRetryer))));
    }

    private static List<ModelActionHolder> createNewTaskModelAction(TitusChangeAction.Builder changeActionBuilder, BatchJobTask newTask, Retryer nextTaskRetryer) {
        List<ModelActionHolder> actions = new ArrayList<>();

        TitusModelAction.Builder modelBuilder = TitusModelAction.newModelUpdate(changeActionBuilder).summary("Creating new task entity holder");
        actions.add(ModelActionHolder.reference(modelBuilder.addTaskHolder(
                EntityHolder.newRoot(newTask.getId(), newTask).addTag(TaskRetryers.ATTR_TASK_RETRY, nextTaskRetryer)
        )));
        actions.add(ModelActionHolder.store(modelBuilder.taskUpdate(newTask, JobManagerConstants.RECONCILER_CALLMETADATA.toBuilder().withCallReason("Creating new task entity holder").build())));

        return actions;
    }

    private static List<ModelActionHolder> createTaskResubmitModelActions(TitusChangeAction.Builder changeActionBuilder, BatchJobTask oldTask, BatchJobTask newTask, Retryer nextTaskRetryer) {
        List<ModelActionHolder> actions = new ArrayList<>();

        TitusModelAction removeTaskAction = TitusModelAction.newModelUpdate(changeActionBuilder)
                .summary("Removing replaced task: " + oldTask.getId())
                .removeTask(oldTask);
        actions.addAll(ModelActionHolder.allModels(removeTaskAction));

        boolean shouldRetry = !TaskStatus.REASON_NORMAL.equals(oldTask.getStatus().getReasonCode());
        if (shouldRetry) {
            actions.addAll(createNewTaskModelAction(changeActionBuilder, newTask, nextTaskRetryer));
        }
        return actions;
    }

    private static BatchJobTask createNewBatchTask(Job<?> job, int index, long timestamp, Map<String, String> taskContext) {
        String taskId = UUID.randomUUID().toString();
        return BatchJobTask.newBuilder()
                .withId(taskId)
                .withJobId(job.getId())
                .withIndex(index)
                .withStatus(TaskStatus.newBuilder().withState(TaskState.Accepted).withTimestamp(timestamp).build())
                .withOriginalId(taskId)
                .withCellInfo(job)
                .addAllToTaskContext(taskContext)
                .build();
    }

    private static BatchJobTask createBatchTaskReplacement(BatchJobTask oldTask, Clock clock, Map<String, String> taskContext) {
        String taskId = UUID.randomUUID().toString();
        return BatchJobTask.newBuilder()
                .withId(taskId)
                .withJobId(oldTask.getJobId())
                .withIndex(oldTask.getIndex())
                .withStatus(TaskStatus.newBuilder().withState(TaskState.Accepted).withTimestamp(clock.wallTime()).build())
                .withOriginalId(oldTask.getOriginalId())
                .withCellInfo(oldTask)
                .withResubmitOf(oldTask.getId())
                .withResubmitNumber(oldTask.getResubmitNumber() + 1)
                .withSystemResubmitNumber(TaskStatus.isSystemError(oldTask.getStatus()) ? oldTask.getSystemResubmitNumber() + 1 : oldTask.getSystemResubmitNumber())
                .withEvictionResubmitNumber(TaskStatus.isEvicted(oldTask) ? oldTask.getEvictionResubmitNumber() + 1 : oldTask.getEvictionResubmitNumber())
                .addAllToTaskContext(taskContext)
                .build();
    }
}
