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

package com.netflix.titus.master.jobmanager.service.service.action;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import com.netflix.titus.api.jobmanager.model.CallMetadata;
import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.api.jobmanager.service.V3JobOperations.Trigger;
import com.netflix.titus.api.jobmanager.store.JobStore;
import com.netflix.titus.common.framework.reconciler.EntityHolder;
import com.netflix.titus.common.framework.reconciler.ModelActionHolder;
import com.netflix.titus.common.framework.reconciler.MultiEngineChangeAction;
import com.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import com.netflix.titus.master.jobmanager.service.JobManagerConstants;
import com.netflix.titus.master.jobmanager.service.common.action.TitusModelAction;
import com.netflix.titus.master.jobmanager.service.event.JobManagerReconcilerEvent;
import rx.Observable;

public class MoveTaskBetweenJobsAction implements MultiEngineChangeAction {

    private final ReconciliationEngine<JobManagerReconcilerEvent> engineFrom;
    private final ReconciliationEngine<JobManagerReconcilerEvent> engineTo;
    private final String taskId;
    private final JobStore titusStore;
    private final CallMetadata callMetadata;

    public MoveTaskBetweenJobsAction(ReconciliationEngine<JobManagerReconcilerEvent> engineFrom,
                                     ReconciliationEngine<JobManagerReconcilerEvent> engineTo,
                                     String taskId,
                                     JobStore titusStore,
                                     CallMetadata callMetadata) {
        this.engineFrom = engineFrom;
        this.engineTo = engineTo;
        this.taskId = taskId;
        this.titusStore = titusStore;
        this.callMetadata = callMetadata;
    }

    @Override
    public Observable<Map<String, List<ModelActionHolder>>> apply() {
        return Observable.defer(() -> {
            // Validate data
            Job<ServiceJobExt> jobFrom = engineFrom.getReferenceView().getEntity();
            Job<ServiceJobExt> jobTo = engineTo.getReferenceView().getEntity();

            EntityHolder taskFromReferenceHolder = engineFrom.getReferenceView().findChildById(taskId)
                    .orElseThrow(() -> JobManagerException.taskJobMismatch(taskId, jobFrom.getId()));

            if (jobFrom.getStatus().getState() != JobState.Accepted) {
                throw JobManagerException.unexpectedJobState(jobTo, JobState.Accepted);
            }
            Capacity capacityFrom = jobFrom.getJobDescriptor().getExtensions().getCapacity();
            if (capacityFrom.getMin() >= capacityFrom.getDesired()) {
                throw JobManagerException.belowMinCapacity(jobFrom, 1);
            }
            if (jobTo.getStatus().getState() != JobState.Accepted) {
                throw JobManagerException.unexpectedJobState(jobTo, JobState.Accepted);
            }
            Capacity capacityTo = jobTo.getJobDescriptor().getExtensions().getCapacity();
            if (capacityTo.getDesired() >= capacityTo.getMax()) {
                throw JobManagerException.aboveMaxCapacity(jobTo, 1);
            }

            Task taskFromReference = taskFromReferenceHolder.getEntity();
            Optional<EntityHolder> taskFromRunningHolder = engineFrom.getRunningView().findChildById(taskId);

            // Compute new model entities

            // Decrement job size by 1
            Job<ServiceJobExt> updatedJobFrom = JobFunctions.incrementJobSize(jobFrom, -1);
            Job<ServiceJobExt> updatedJobTo = JobFunctions.incrementJobSize(jobTo, 1);
            Task updatedReferenceTaskTo = JobFunctions.moveTask(jobFrom.getId(), jobTo.getId(), taskFromReference);

            // Move the task
            return titusStore.moveTask(updatedJobFrom, updatedJobTo, updatedReferenceTaskTo).andThen(
                    Observable.fromCallable(() -> ImmutableMap.of(
                            jobFrom.getId(), createModelUpdateActionsFrom(updatedJobFrom, updatedJobTo, taskFromReference),
                            jobTo.getId(), createModelUpdateActionsTo(updatedJobFrom, updatedJobTo, updatedReferenceTaskTo, taskFromRunningHolder)
                    ))
            );
        });
    }

    private List<ModelActionHolder> createModelUpdateActionsFrom(Job<ServiceJobExt> updatedJobFrom, Job<ServiceJobExt> updatedJobTo, Task taskFrom) {
        List<ModelActionHolder> actions = new ArrayList<>();

        // Remove task from all models.
        TitusModelAction removeTaskAction = TitusModelAction.newModelUpdate("moveTask")
                .job(updatedJobFrom)
                .trigger(Trigger.API)
                .summary("Task moved to another job: jobTo=" + updatedJobTo.getId())
                .removeTask(taskFrom);
        actions.addAll(ModelActionHolder.allModels(removeTaskAction));
        String summary = "Decremented the desired job size by one, as its task was moved to another job: jobTo=" + updatedJobTo.getId();
        // Change job size
        TitusModelAction modelAction = TitusModelAction.newModelUpdate("decrementJobSize")
                .job(updatedJobFrom)
                .trigger(Trigger.API)
                .summary(summary)
                .jobUpdate(jobHolder -> jobHolder.setEntity(updatedJobFrom).addTag(JobManagerConstants.JOB_MANAGER_ATTRIBUTE_CALLMETADATA, callMetadata.toBuilder().withCallReason(summary).build()));
        actions.addAll(ModelActionHolder.referenceAndStore(modelAction));

        return actions;
    }

    private List<ModelActionHolder> createModelUpdateActionsTo(Job<ServiceJobExt> updatedJobFrom, Job<?> updatedJobTo, Task taskToUpdated, Optional<EntityHolder> taskFromRunningHolder) {
        List<ModelActionHolder> actions = new ArrayList<>();

        // Add task
        TitusModelAction addTaskAction = TitusModelAction.newModelUpdate("moveTask")
                .job(updatedJobTo)
                .trigger(Trigger.API)
                .summary("Received task from another job: jobFrom=" + updatedJobFrom.getId())
                .taskUpdate(taskToUpdated, CallMetadata.newBuilder().withCallReason("test").withCallerId("test").build());

        if (taskFromRunningHolder.isPresent()) {
            actions.addAll(ModelActionHolder.allModels(addTaskAction));
        } else {
            actions.addAll(ModelActionHolder.referenceAndStore(addTaskAction));
        }

        // Change job size
        TitusModelAction modelAction = TitusModelAction.newModelUpdate("incrementJobSize")
                .job(updatedJobTo)
                .trigger(Trigger.API)
                .summary("Incremented the desired job size by one, as it got a task from another job: jobFrom=" + updatedJobFrom.getId())
                .jobUpdate(jobHolder -> jobHolder.setEntity(updatedJobTo).addTag(JobManagerConstants.JOB_MANAGER_ATTRIBUTE_CALLMETADATA,
                        callMetadata.toBuilder().withCallReason("Incremented the desired job size by one, as it got a task from another job").build()));
        actions.addAll(ModelActionHolder.referenceAndStore(modelAction));

        return actions;
    }
}
