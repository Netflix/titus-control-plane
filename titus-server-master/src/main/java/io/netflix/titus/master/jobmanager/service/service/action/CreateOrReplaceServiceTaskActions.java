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
import java.util.UUID;

import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobFunctions;
import io.netflix.titus.api.jobmanager.model.job.ServiceJobTask;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.api.jobmanager.store.JobStore;
import io.netflix.titus.common.framework.reconciler.EntityHolder;
import io.netflix.titus.common.framework.reconciler.ModelActionHolder;
import io.netflix.titus.common.util.retry.Retryer;
import io.netflix.titus.common.util.time.Clock;
import io.netflix.titus.master.jobmanager.service.JobManagerConfiguration;
import io.netflix.titus.master.jobmanager.service.common.action.TaskRetryers;
import io.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.master.jobmanager.service.common.action.TitusModelAction;
import rx.Observable;

/**
 * Create a new task or replace a completed task, and persist it into the store. Update reference/store models.
 */
public class CreateOrReplaceServiceTaskActions {

    public static TitusChangeAction createOrReplaceTaskAction(JobManagerConfiguration configuration, JobStore jobStore, EntityHolder jobHolder, Optional<EntityHolder> previousTaskHolder, Clock clock) {
        Job<ServiceJobExt> job = jobHolder.getEntity();
        return previousTaskHolder
                .map(previous -> createResubmittedTaskChangeAction(jobHolder, previous, configuration, jobStore, clock))
                .orElseGet(() -> createOriginalTaskChangeAction(job, jobStore, clock));
    }

    private static TitusChangeAction createOriginalTaskChangeAction(Job<ServiceJobExt> job, JobStore jobStore, Clock clock) {
        Retryer newRetryer = JobFunctions.retryer(job);
        ServiceJobTask newTask = createNewServiceTask(job, clock.wallTime());
        String summary = String.format("Creating new service task in DB store: %s", newTask.getId());

        return TitusChangeAction.newAction("createOrReplaceTask")
                .id(newTask.getId())
                .trigger(V3JobOperations.Trigger.Reconciler)
                .summary(summary)
                .changeWithModelUpdates(self -> jobStore.storeTask(newTask).andThen(Observable.just(createNewTaskModelAction(self, newTask, newRetryer))));
    }

    private static TitusChangeAction createResubmittedTaskChangeAction(EntityHolder jobHolder, EntityHolder taskHolder, JobManagerConfiguration configuration, JobStore jobStore, Clock clock) {
        ServiceJobTask oldTask = taskHolder.getEntity();
        long timeInStartedState = JobFunctions.getTimeInState(oldTask, TaskState.Started, clock).orElse(0L);
        Retryer nextTaskRetryer = timeInStartedState >= configuration.getTaskRetryerResetTimeMs()
                ? JobFunctions.retryer(jobHolder.getEntity())
                : TaskRetryers.getNextTaskRetryer(jobHolder.getEntity(), taskHolder);
        ServiceJobTask newTask = createServiceTaskReplacement(oldTask, clock.wallTime());

        String summary = String.format(
                "Replacing service task in DB store: resubmit=%d, originalId=%s, previousId=%s, newId=%s",
                newTask.getResubmitNumber(),
                oldTask.getOriginalId(),
                oldTask.getId(),
                newTask.getId()
        );

        return TitusChangeAction.newAction("createOrReplaceTask")
                .id(newTask.getId())
                .trigger(V3JobOperations.Trigger.Reconciler)
                .summary(summary)
                .changeWithModelUpdates(self -> jobStore.replaceTask(oldTask, newTask).andThen(Observable.just(createTaskResubmitModelActions(self, oldTask, newTask, nextTaskRetryer))));
    }

    private static List<ModelActionHolder> createNewTaskModelAction(TitusChangeAction.Builder changeActionBuilder, ServiceJobTask newTask, Retryer newRetryer) {
        List<ModelActionHolder> actions = new ArrayList<>();

        TitusModelAction.Builder modelBuilder = TitusModelAction.newModelUpdate(changeActionBuilder).summary("Creating new task entity holder");
        actions.add(ModelActionHolder.reference(modelBuilder.addTaskHolder(
                EntityHolder.newRoot(newTask.getId(), newTask).addTag(TaskRetryers.ATTR_TASK_RETRY, newRetryer)
        )));
        actions.add(ModelActionHolder.store(modelBuilder.taskUpdate(newTask)));

        return actions;
    }

    private static List<ModelActionHolder> createTaskResubmitModelActions(TitusChangeAction.Builder changeActionBuilder, ServiceJobTask oldTask, ServiceJobTask newTask, Retryer nextTaskRetryer) {
        List<ModelActionHolder> actions = new ArrayList<>();

        TitusModelAction removeTaskAction = TitusModelAction.newModelUpdate(changeActionBuilder)
                .summary("Removing replaced task")
                .removeTask(oldTask);
        actions.addAll(ModelActionHolder.allModels(removeTaskAction));
        actions.addAll(createNewTaskModelAction(changeActionBuilder, newTask, nextTaskRetryer));

        return actions;
    }

    private static ServiceJobTask createNewServiceTask(Job<?> job, long timestamp) {
        String taskId = UUID.randomUUID().toString();
        return ServiceJobTask.newBuilder()
                .withId(taskId)
                .withJobId(job.getId())
                .withStatus(TaskStatus.newBuilder().withState(TaskState.Accepted).withTimestamp(timestamp).build())
                .withOriginalId(taskId)
                .withCellInfo(job)
                .build();
    }

    private static ServiceJobTask createServiceTaskReplacement(ServiceJobTask oldTask, long timestamp) {
        String taskId = UUID.randomUUID().toString();
        return ServiceJobTask.newBuilder()
                .withId(taskId)
                .withJobId(oldTask.getJobId())
                .withStatus(TaskStatus.newBuilder().withState(TaskState.Accepted).withTimestamp(timestamp).build())
                .withOriginalId(oldTask.getOriginalId())
                .withCellInfo(oldTask)
                .withResubmitOf(oldTask.getId())
                .withResubmitNumber(oldTask.getResubmitNumber() + 1)
                .withSystemResubmitNumber(TaskStatus.isSystemError(oldTask.getStatus()) ? oldTask.getSystemResubmitNumber() + 1 : oldTask.getSystemResubmitNumber())
                .build();
    }
}
