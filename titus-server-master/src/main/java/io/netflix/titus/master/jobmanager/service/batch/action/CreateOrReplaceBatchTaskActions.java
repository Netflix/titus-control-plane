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

package io.netflix.titus.master.jobmanager.service.batch.action;

import java.util.ArrayList;
import java.util.List;

import io.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobFunctions;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import io.netflix.titus.api.jobmanager.service.V3JobOperations.Trigger;
import io.netflix.titus.api.jobmanager.store.JobStore;
import io.netflix.titus.common.framework.reconciler.ModelActionHolder;
import io.netflix.titus.common.util.retry.Retryer;
import io.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.master.jobmanager.service.common.action.TitusModelAction;
import rx.Observable;

import static io.netflix.titus.master.jobmanager.service.common.action.TitusModelUpdateActions.createTask;
import static io.netflix.titus.master.jobmanager.service.common.action.TitusModelUpdateActions.removeTask;

/**
 * Create a new task or replace a completed task, and persist it into the store. Update reference/store models.
 */
public class CreateOrReplaceBatchTaskActions {

    private static final String ATTR_TASK_RETRY = "task.retry.";

    public static TitusChangeAction createOrReplaceTaskAction(JobStore jobStore, Job<BatchJobExt> job, List<BatchJobTask> tasks, int index) {
        return tasks.stream().filter(t -> t.getIndex() == index).findFirst()
                .map(oldTask -> createResubmittedTaskChangeAction(oldTask, jobStore))
                .orElseGet(() -> createOriginalTaskChangeAction(job, index, jobStore));
    }

    private static TitusChangeAction createOriginalTaskChangeAction(Job<BatchJobExt> job, int index, JobStore jobStore) {
        BatchJobTask newTask = JobFunctions.createNewBatchTask(job, index);
        String summary = String.format("Creating new task at index %d in DB store: %s", newTask.getIndex(), newTask.getId());

        return TitusChangeAction.newAction("createOrReplaceTask")
                .id(newTask.getId())
                .trigger(Trigger.Reconciler)
                .summary(summary)
                .changeWithModelUpdates(self -> jobStore.storeTask(newTask).andThen(Observable.just(createOriginalTaskModelAction(self, newTask))));
    }

    private static TitusChangeAction createResubmittedTaskChangeAction(BatchJobTask oldTask, JobStore jobStore) {
        BatchJobTask newTask = JobFunctions.createBatchTaskReplacement(oldTask);
        String summary = String.format("Replacing task at index %d in DB store: old=%s, new=%s", oldTask.getIndex(), oldTask.getId(), newTask.getId());

        return TitusChangeAction.newAction("createOrReplaceTask")
                .id(newTask.getId())
                .trigger(Trigger.Reconciler)
                .summary(summary)
                .changeWithModelUpdates(self -> jobStore.replaceTask(oldTask, newTask).andThen(Observable.just(createTaskResubmitModelActions(self, oldTask, newTask))));
    }

    private static List<ModelActionHolder> createOriginalTaskModelAction(TitusChangeAction.Builder changeActionBuilder, BatchJobTask newTask) {
        List<ModelActionHolder> actions = new ArrayList<>();
        actions.addAll(ModelActionHolder.referenceAndStore(createTask(newTask, Trigger.Reconciler, "Creating new task entity holder")));
        actions.add(ModelActionHolder.reference(createOrUpdateTaskRetryer(changeActionBuilder, newTask)));
        return actions;
    }

    private static List<ModelActionHolder> createTaskResubmitModelActions(TitusChangeAction.Builder changeActionBuilder, BatchJobTask oldTask, BatchJobTask newTask) {
        List<ModelActionHolder> actions = new ArrayList<>();
        actions.addAll(ModelActionHolder.allModels(removeTask(oldTask.getId(), Trigger.Reconciler, "Removing replaced task")));
        boolean shouldRetry = !TaskStatus.REASON_NORMAL.equals(oldTask.getStatus().getReasonCode());
        if (shouldRetry) {
            actions.addAll(ModelActionHolder.referenceAndStore(createTask(newTask, Trigger.Reconciler, "Creating new task entity holder")));
            actions.add(ModelActionHolder.reference(createOrUpdateTaskRetryer(changeActionBuilder, newTask)));
        }
        return actions;
    }

    private static TitusModelAction createOrUpdateTaskRetryer(TitusChangeAction.Builder changeActionBuilder, BatchJobTask task) {
        return TitusModelAction.newModelUpdate(changeActionBuilder)
                .summary("Updating retry execution status for task index " + task.getIndex())
                .jobUpdate(jobHolder -> {
                    String tagName = getRetryerAttribute(task);
                    Retryer retryer = (Retryer) jobHolder.getAttributes().get(tagName);
                    Retryer newRetryer = retryer == null ? JobFunctions.retryer(jobHolder.getEntity(), task) : retryer.retry();
                    return jobHolder.addTag(tagName, newRetryer);
                });
    }

    private static String getRetryerAttribute(BatchJobTask task) {
        return ATTR_TASK_RETRY + task.getIndex();
    }
}
