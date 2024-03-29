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

package com.netflix.titus.master.jobmanager.service.service.action;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.LogStorageInfos;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobTask;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.jobmanager.store.JobStore;
import com.netflix.titus.common.framework.reconciler.EntityHolder;
import com.netflix.titus.common.framework.reconciler.ModelActionHolder;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.retry.Retryer;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.master.jobmanager.service.JobServiceRuntime;
import com.netflix.titus.master.jobmanager.service.VersionSupplier;
import com.netflix.titus.master.jobmanager.service.common.action.TaskRetryers;
import com.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import com.netflix.titus.master.jobmanager.service.common.action.TitusModelAction;
import rx.Observable;

import static com.netflix.titus.master.jobmanager.service.common.action.JobEntityHolders.ATTR_REPLACEMENT_OF;

/**
 * Create a new task or replace a completed task, and persist it into the store. Update reference/store models.
 */
public class CreateOrReplaceServiceTaskActions {

    public static TitusChangeAction createOrReplaceTaskAction(JobServiceRuntime context,
                                                              JobStore jobStore,
                                                              VersionSupplier versionSupplier,
                                                              EntityHolder jobHolder,
                                                              Optional<EntityHolder> previousTaskHolder,
                                                              Clock clock,
                                                              Map<String, String> taskContext) {
        Job<ServiceJobExt> job = jobHolder.getEntity();
        return previousTaskHolder
                .map(previous -> createResubmittedTaskChangeAction(jobHolder, previous, context, jobStore, versionSupplier, clock, taskContext))
                .orElseGet(() -> createOriginalTaskChangeAction(job, jobStore, versionSupplier, clock, taskContext));
    }

    private static TitusChangeAction createOriginalTaskChangeAction(Job<ServiceJobExt> job, JobStore jobStore,
                                                                    VersionSupplier versionSupplier, Clock clock, Map<String, String> taskContext) {
        Retryer newRetryer = JobFunctions.retryer(job);
        ServiceJobTask newTask = createNewServiceTask(job, clock.wallTime(), taskContext, versionSupplier);
        String summary = String.format("Creating new service task in DB store: %s", newTask.getId());

        return TitusChangeAction.newAction("createOrReplaceTask")
                .id(newTask.getId())
                .trigger(V3JobOperations.Trigger.Reconciler)
                .summary(summary)
                .changeWithModelUpdates(self -> jobStore.storeTask(newTask).andThen(Observable.just(createNewTaskModelAction(self, newTask, Optional.empty(), newRetryer))));
    }

    private static TitusChangeAction createResubmittedTaskChangeAction(EntityHolder jobHolder,
                                                                       EntityHolder taskHolder,
                                                                       JobServiceRuntime context,
                                                                       JobStore jobStore,
                                                                       VersionSupplier versionSupplier,
                                                                       Clock clock,
                                                                       Map<String, String> taskContext) {
        ServiceJobTask oldTask = taskHolder.getEntity();
        long timeInStartedState = JobFunctions.getTimeInState(oldTask, TaskState.Started, clock).orElse(0L);
        Retryer nextTaskRetryer = timeInStartedState >= context.getConfiguration().getTaskRetryerResetTimeMs()
                ? JobFunctions.retryer(jobHolder.getEntity())
                : TaskRetryers.getNextTaskRetryer(context::getSystemRetryer, jobHolder.getEntity(), taskHolder);
        ServiceJobTask newTask = createServiceTaskReplacement(jobHolder.getEntity(), oldTask, clock.wallTime(), taskContext, versionSupplier);

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

    private static List<ModelActionHolder> createNewTaskModelAction(TitusChangeAction.Builder changeActionBuilder,
                                                                    ServiceJobTask newTask,
                                                                    Optional<ServiceJobTask> replacementOf,
                                                                    Retryer newRetryer) {
        List<ModelActionHolder> actions = new ArrayList<>();
        String summary = "Creating new task entity holder";

        TitusModelAction.Builder modelBuilder = TitusModelAction.newModelUpdate(changeActionBuilder).summary(summary);
        EntityHolder taskHolder = EntityHolder.newRoot(newTask.getId(), newTask).addTag(TaskRetryers.ATTR_TASK_RETRY, newRetryer);
        if (replacementOf.isPresent()) {
            taskHolder = taskHolder.addTag(ATTR_REPLACEMENT_OF, replacementOf.get());
        }
        actions.add(ModelActionHolder.reference(modelBuilder.addTaskHolder(taskHolder)));
        actions.add(ModelActionHolder.store(modelBuilder.taskUpdate(newTask)));

        return actions;
    }

    private static List<ModelActionHolder> createTaskResubmitModelActions(TitusChangeAction.Builder changeActionBuilder, ServiceJobTask oldTask, ServiceJobTask newTask, Retryer nextTaskRetryer) {
        List<ModelActionHolder> actions = new ArrayList<>();

        TitusModelAction removeTaskAction = TitusModelAction.newModelUpdate(changeActionBuilder)
                .summary("Removing replaced task")
                .removeTask(oldTask);
        actions.addAll(ModelActionHolder.allModels(removeTaskAction));
        actions.addAll(createNewTaskModelAction(changeActionBuilder, newTask, Optional.of(oldTask), nextTaskRetryer));

        return actions;
    }

    private static ServiceJobTask createNewServiceTask(Job<?> job, long timestamp, Map<String, String> taskContext, VersionSupplier versionSupplier) {
        String taskId = UUID.randomUUID().toString();
        return ServiceJobTask.newBuilder()
                .withId(taskId)
                .withJobId(job.getId())
                .withStatus(TaskStatus.newBuilder().withState(TaskState.Accepted).withReasonCode(TaskStatus.REASON_NORMAL).withTimestamp(timestamp).build())
                .withOriginalId(taskId)
                .withCellInfo(job)
                .addAllToTaskContext(CollectionsExt.merge(taskContext, LogStorageInfos.toS3LogLocationTaskContext(job)))
                .withVersion(versionSupplier.nextVersion())
                .build();
    }

    private static ServiceJobTask createServiceTaskReplacement(Job<?> job, ServiceJobTask oldTask, long timestamp,
                                                               Map<String, String> taskContext, VersionSupplier versionSupplier) {
        String taskId = UUID.randomUUID().toString();
        return ServiceJobTask.newBuilder()
                .withId(taskId)
                .withJobId(oldTask.getJobId())
                .withStatus(TaskStatus.newBuilder().withState(TaskState.Accepted).withReasonCode(TaskStatus.REASON_NORMAL).withTimestamp(timestamp).build())
                .withOriginalId(oldTask.getOriginalId())
                .withCellInfo(oldTask)
                .withResubmitOf(oldTask.getId())
                .withResubmitNumber(oldTask.getResubmitNumber() + 1)
                .withSystemResubmitNumber(TaskStatus.hasSystemError(oldTask) ? oldTask.getSystemResubmitNumber() + 1 : oldTask.getSystemResubmitNumber())
                .withEvictionResubmitNumber(TaskStatus.isEvicted(oldTask
                ) ? oldTask.getEvictionResubmitNumber() + 1 : oldTask.getEvictionResubmitNumber())
                .addAllToTaskContext(CollectionsExt.merge(taskContext, LogStorageInfos.toS3LogLocationTaskContext(job)))
                .withVersion(versionSupplier.nextVersion())
                .build();
    }
}
