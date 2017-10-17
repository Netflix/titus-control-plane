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
import java.util.Optional;
import java.util.UUID;

import io.netflix.titus.api.jobmanager.model.event.JobManagerEvent.Trigger;
import io.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import io.netflix.titus.api.jobmanager.model.job.retry.RetryPolicy;
import io.netflix.titus.api.jobmanager.service.common.action.ActionKind;
import io.netflix.titus.api.jobmanager.service.common.action.JobChange;
import io.netflix.titus.api.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.api.jobmanager.service.common.action.TitusModelUpdateAction;
import io.netflix.titus.api.jobmanager.store.JobStore;
import io.netflix.titus.common.framework.reconciler.ModelUpdateAction;
import io.netflix.titus.common.framework.reconciler.ModelUpdateAction.Model;
import io.netflix.titus.common.util.retry.Retryer;
import io.netflix.titus.common.util.tuple.Pair;
import rx.Observable;

import static io.netflix.titus.api.jobmanager.model.job.JobFunctions.retryerFrom;
import static io.netflix.titus.master.jobmanager.service.common.action.TitusModelUpdateActions.createTask;
import static io.netflix.titus.master.jobmanager.service.common.action.TitusModelUpdateActions.removeTask;
import static io.netflix.titus.master.jobmanager.service.common.action.TitusModelUpdateActions.updateJobHolder;

/**
 * Create a new task or replace a completed task, and persist it into the store. Update reference/store models.
 */
public class CreateOrReplaceBatchTaskAction extends TitusChangeAction {

    private static final String ATTR_TASK_RETRY = "task.retry.";

    private final JobStore titusStore;
    private final BatchJobTask newTask;
    private final Optional<BatchJobTask> oldTaskOpt;

    private CreateOrReplaceBatchTaskAction(JobStore titusStore,
                                           BatchJobTask newTask,
                                           Optional<BatchJobTask> oldTaskOpt,
                                           String summary) {
        super(new JobChange(ActionKind.Task, Trigger.Reconciler, newTask.getId(), summary));
        this.titusStore = titusStore;
        this.newTask = newTask;
        this.oldTaskOpt = oldTaskOpt;
    }

    @Override
    public Observable<Pair<JobChange, List<ModelUpdateAction>>> apply() {
        if (oldTaskOpt.isPresent()) {
            return titusStore.replaceTask(oldTaskOpt.get(), newTask).andThen(Observable.just(Pair.of(getChange(), createTaskReplaceUpdateActions())));
        }
        return titusStore.storeTask(newTask).andThen(Observable.just(Pair.of(getChange(), createTaskReplaceUpdateActions())));
    }

    private TitusModelUpdateAction createOrUpdateTaskRetryer(BatchJobTask task) {
        return updateJobHolder(task.getJobId(), jobHolder -> {
            String tagName = getRetryerAttribute(task);
            Retryer retryer = (Retryer) jobHolder.getAttributes().get(tagName);

            Retryer newRetryer;
            if (retryer == null) {
                Job<BatchJobExt> job = jobHolder.getEntity();
                RetryPolicy retryPolicy = job.getJobDescriptor().getExtensions().getRetryPolicy();
                int remainingRetries = retryPolicy.getRetries() - task.getResubmitNumber();
                newRetryer = retryerFrom(retryPolicy, remainingRetries);
            } else {
                newRetryer = retryer.retry();
            }

            return jobHolder.addTag(tagName, newRetryer);
        }, Trigger.Reconciler, Model.Reference, "Updating retry execution status for task index " + task.getIndex());
    }

    private List<ModelUpdateAction> createTaskReplaceUpdateActions() {
        List<ModelUpdateAction> actions = new ArrayList<>();

        oldTaskOpt.ifPresent(oldTask -> {
            actions.add(removeTask(oldTask.getId(), Trigger.Reconciler, Model.Reference, "Removing replaced task"));
            actions.add(removeTask(oldTask.getId(), Trigger.Reconciler, Model.Running, "Removing replaced task"));
            actions.add(removeTask(oldTask.getId(), Trigger.Reconciler, Model.Store, "Removing replaced task"));
        });
        boolean shouldRetry = oldTaskOpt.map(oldTask -> TaskStatus.REASON_TASK_KILLED.equals(oldTask.getStatus().getReasonCode())).orElse(true);
        if (shouldRetry) {
            actions.add(createTask(newTask, Trigger.Reconciler, Model.Reference, "Creating new task"));
            actions.add(createTask(newTask, Trigger.Reconciler, Model.Store, "Creating new task"));
            actions.add(createOrUpdateTaskRetryer(newTask));
        }

        return actions;
    }

    public static String getRetryerAttribute(BatchJobTask task) {
        return ATTR_TASK_RETRY + task.getIndex();
    }

    public static TitusChangeAction createOrReplaceTaskAction(JobStore titusStore, Job<BatchJobExt> job, List<BatchJobTask> tasks, int index) {
        String taskId = UUID.randomUUID().toString();

        Optional<BatchJobTask> oldTaskOpt = tasks.stream().filter(t -> t.getIndex() == index).findFirst();
        BatchJobTask newTask = createNewTask(job, oldTaskOpt, index, taskId);

        String summary = oldTaskOpt
                .map(oldTask -> String.format("Replacing task at index %d: old=%s, new=%s", oldTask.getIndex(), oldTask.getId(), newTask.getId()))
                .orElseGet(() -> String.format("Creating new task at index %d: %s", newTask.getIndex(), newTask.getId()));

        return new CreateOrReplaceBatchTaskAction(titusStore, newTask, oldTaskOpt, summary);
    }

    private static BatchJobTask createNewTask(Job<BatchJobExt> job, Optional<BatchJobTask> oldTaskOpt, int index, String newTaskId) {
        BatchJobTask.Builder builder = BatchJobTask.newBuilder()
                .withId(newTaskId)
                .withJobId(job.getId())
                .withIndex(index)
                .withStatus(TaskStatus.newBuilder().withState(TaskState.Accepted).build());

        if (oldTaskOpt.isPresent()) {
            BatchJobTask oldTask = oldTaskOpt.get();
            builder.withOriginalId(oldTask.getOriginalId())
                    .withResubmitOf(oldTask.getId())
                    .withResubmitNumber(oldTask.getResubmitNumber() + 1);
        } else {
            builder.withOriginalId(newTaskId);
        }

        return builder.build();
    }
}
