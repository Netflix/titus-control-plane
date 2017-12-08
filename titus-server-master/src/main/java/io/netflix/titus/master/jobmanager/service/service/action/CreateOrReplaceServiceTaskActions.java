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

package io.netflix.titus.master.jobmanager.service.service.action;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobFunctions;
import io.netflix.titus.api.jobmanager.model.job.ServiceJobTask;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.api.jobmanager.store.JobStore;
import io.netflix.titus.common.framework.reconciler.ModelActionHolder;
import io.netflix.titus.common.util.retry.Retryer;
import io.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.master.jobmanager.service.common.action.TitusModelAction;
import rx.Observable;

/**
 * Create a new task or replace a completed task, and persist it into the store. Update reference/store models.
 */
public class CreateOrReplaceServiceTaskActions {

    private static final String ATTR_TASK_RETRY = "task.retry.";

    public static TitusChangeAction createOrReplaceTaskAction(JobStore jobStore, Job<ServiceJobExt> job, Optional<ServiceJobTask> previousTask) {
        return previousTask
                .map(previous -> createResubmittedTaskChangeAction(previous, jobStore))
                .orElseGet(() -> createOriginalTaskChangeAction(job, jobStore));
    }

    private static TitusChangeAction createOriginalTaskChangeAction(Job<ServiceJobExt> job, JobStore jobStore) {
        ServiceJobTask newTask = JobFunctions.createNewServiceTask(job);
        String summary = String.format("Creating new service task in DB store: %s", newTask.getId());

        return TitusChangeAction.newAction("createOrReplaceTask")
                .id(newTask.getId())
                .trigger(V3JobOperations.Trigger.Reconciler)
                .summary(summary)
                .changeWithModelUpdates(self -> jobStore.storeTask(newTask).andThen(Observable.just(createOriginalTaskModelAction(self, newTask))));
    }

    private static TitusChangeAction createResubmittedTaskChangeAction(ServiceJobTask oldTask, JobStore jobStore) {
        ServiceJobTask newTask = JobFunctions.createServiceTaskReplacement(oldTask);
        String summary = String.format("Replacing service task in DB store: originalId=%s, previousId=%s, newId=%s", oldTask.getOriginalId(), oldTask.getId(), newTask.getId());

        return TitusChangeAction.newAction("createOrReplaceTask")
                .id(newTask.getId())
                .trigger(V3JobOperations.Trigger.Reconciler)
                .summary(summary)
                .changeWithModelUpdates(self -> jobStore.replaceTask(oldTask, newTask).andThen(Observable.just(createTaskResubmitModelActions(self, oldTask, newTask))));
    }

    private static List<ModelActionHolder> createOriginalTaskModelAction(TitusChangeAction.Builder changeActionBuilder, ServiceJobTask newTask) {
        List<ModelActionHolder> actions = new ArrayList<>();

        TitusModelAction newTaskModelAction = TitusModelAction.newModelUpdate(changeActionBuilder)
                .summary("Creating new task entity holder")
                .addTask(newTask);
        actions.addAll(ModelActionHolder.referenceAndStore(newTaskModelAction));

        actions.add(ModelActionHolder.reference(createOrUpdateTaskRetryer(changeActionBuilder, newTask)));
        return actions;
    }

    private static List<ModelActionHolder> createTaskResubmitModelActions(TitusChangeAction.Builder changeActionBuilder, ServiceJobTask oldTask, ServiceJobTask newTask) {
        List<ModelActionHolder> actions = new ArrayList<>();

        TitusModelAction removeTaskAction = TitusModelAction.newModelUpdate(changeActionBuilder)
                .task(oldTask)
                .summary("Removing replaced task")
                .removeTask(oldTask);
        actions.addAll(ModelActionHolder.allModels(removeTaskAction));

        TitusModelAction newTaskModelAction = TitusModelAction.newModelUpdate(changeActionBuilder)
                .summary("Creating new task entity holder")
                .addTask(newTask);
        actions.addAll(ModelActionHolder.referenceAndStore(newTaskModelAction));

        actions.add(ModelActionHolder.reference(createOrUpdateTaskRetryer(changeActionBuilder, newTask)));
        return actions;
    }

    private static TitusModelAction createOrUpdateTaskRetryer(TitusChangeAction.Builder changeActionBuilder, ServiceJobTask task) {
        return TitusModelAction.newModelUpdate(changeActionBuilder)
                .summary("Updating retry execution status for service task")
                .jobUpdate(jobHolder -> {
                    String tagName = getRetryerAttribute(task);
                    Retryer retryer = (Retryer) jobHolder.getAttributes().get(tagName);
                    Retryer newRetryer = retryer == null ? JobFunctions.retryer(jobHolder.getEntity(), task) : retryer.retry();
                    return jobHolder.addTag(tagName, newRetryer);
                });
    }

    private static String getRetryerAttribute(ServiceJobTask task) {
        return ATTR_TASK_RETRY + task.getOriginalId();
    }
}
