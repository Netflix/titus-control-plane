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

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudget;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.jobmanager.store.JobStore;
import com.netflix.titus.common.framework.reconciler.ChangeAction;
import com.netflix.titus.common.framework.reconciler.EntityHolder;
import com.netflix.titus.common.framework.reconciler.ModelActionHolder;
import com.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import com.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import com.netflix.titus.master.jobmanager.service.common.action.TitusModelAction;
import com.netflix.titus.master.jobmanager.service.event.JobManagerReconcilerEvent;
import rx.Observable;

public class BasicJobActions {

    private static final String ATTR_JOB_CLOSED = "job.closed";

    /**
     * Write an updated job record to a store. This command calls {@link JobStore#updateJob(Job)}, which assumes
     * that the job record was created already.
     */
    public static TitusChangeAction updateJobInStore(ReconciliationEngine<JobManagerReconcilerEvent> engine, JobStore titusStore) {
        return TitusChangeAction.newAction("updateJobInStore")
                .id(engine.getReferenceView().getId())
                .trigger(V3JobOperations.Trigger.Reconciler)
                .summary("Updating job record in store")
                .changeWithModelUpdate(self -> {
                    Job<?> referenceJob = engine.getReferenceView().getEntity();

                    TitusModelAction modelUpdateAction = TitusModelAction.newModelUpdate(self)
                            .jobMaybeUpdate(storeJobHolder -> Optional.of(storeJobHolder.setEntity(referenceJob)));

                    return titusStore.updateJob(referenceJob).andThen(Observable.just(ModelActionHolder.store(modelUpdateAction)));
                });
    }

    /**
     * Update the disruption budget of a job.
     */
    public static ChangeAction updateJobDisruptionBudget(ReconciliationEngine<JobManagerReconcilerEvent> engine, DisruptionBudget disruptionBudget, JobStore jobStore) {
        return TitusChangeAction.newAction("updateDisruptionBudget")
                .id(engine.getReferenceView().getId())
                .trigger(V3JobOperations.Trigger.API)
                .summary("Job disruption budget update")
                .changeWithModelUpdates(self -> {

                    Job<?> job = engine.getReferenceView().getEntity();
                    Job<?> updatedJob = JobFunctions.changeDisruptionBudget(job, disruptionBudget);

                    TitusModelAction modelAction = TitusModelAction.newModelUpdate(self).jobUpdate(jobHolder -> jobHolder.setEntity(updatedJob));

                    return jobStore.updateJob(updatedJob).andThen(Observable.just(ModelActionHolder.referenceAndStore(modelAction)));
                });
    }

    /**
     * Update the attributes of a job. This will either create new attributes or replacing an existing ones with the same key.
     */
    public static ChangeAction updateJobAttributes(ReconciliationEngine<JobManagerReconcilerEvent> engine, Map<String, String> attributes, JobStore jobStore) {
        return TitusChangeAction.newAction("updateJobAttributes")
                .id(engine.getReferenceView().getId())
                .trigger(V3JobOperations.Trigger.API)
                .summary("Update job attributes")
                .changeWithModelUpdates(self -> {
                    Job<?> job = engine.getReferenceView().getEntity();
                    Job<?> updatedJob = JobFunctions.updateJobAttributes(job, attributes);

                    TitusModelAction modelAction = TitusModelAction.newModelUpdate(self).jobUpdate(jobHolder -> jobHolder.setEntity(updatedJob));

                    return jobStore.updateJob(updatedJob).andThen(Observable.just(ModelActionHolder.referenceAndStore(modelAction)));
                });
    }

    /**
     * Delete the attributes of a job with the specified key names.
     */
    public static ChangeAction deleteJobAttributes(ReconciliationEngine<JobManagerReconcilerEvent> engine, List<String> keys, JobStore jobStore) {
        return TitusChangeAction.newAction("updateJobAttributes")
                .id(engine.getReferenceView().getId())
                .trigger(V3JobOperations.Trigger.API)
                .summary("Delete job attributes")
                .changeWithModelUpdates(self -> {
                    Job<?> job = engine.getReferenceView().getEntity();
                    Job<?> updatedJob = JobFunctions.deleteJobAttributes(job, keys);

                    TitusModelAction modelAction = TitusModelAction.newModelUpdate(self).jobUpdate(jobHolder -> jobHolder.setEntity(updatedJob));

                    return jobStore.updateJob(updatedJob).andThen(Observable.just(ModelActionHolder.referenceAndStore(modelAction)));
                });
    }

    /**
     * Move job to {@link JobState#Finished} state in reference and running models.
     */
    public static TitusChangeAction completeJob(String jobId) {
        return TitusChangeAction.newAction("closeJob")
                .id(jobId)
                .trigger(V3JobOperations.Trigger.Reconciler)
                .summary("Moving job to Finished state")
                .applyModelUpdates(self -> {
                    TitusModelAction modelUpdateAction = TitusModelAction.newModelUpdate(self)
                            .jobMaybeUpdate(entityHolder -> {
                                Job job = entityHolder.getEntity();
                                if (job.getStatus().getState() != JobState.Finished) {
                                    Job newJob = JobFunctions.changeJobStatus(job, JobState.Finished, TaskStatus.REASON_NORMAL);
                                    return Optional.of(entityHolder.setEntity(newJob));
                                }
                                return Optional.empty();

                            });
                    return ModelActionHolder.referenceAndRunning(modelUpdateAction);
                });
    }

    /**
     * Delete a job from store, and mark it as closed in the reference model. {@link #ATTR_JOB_CLOSED} attribute
     * is used to identify closed jobs, which can be removed from the reconciliation engine.
     */
    public static TitusChangeAction removeJobFromStore(Job job, JobStore store) {
        return TitusChangeAction.newAction("removeJobFromStore")
                .job(job)
                .trigger(V3JobOperations.Trigger.Reconciler)
                .summary("Removing job from the storage")
                .changeWithModelUpdate(self -> {
                    TitusModelAction modelUpdateAction = TitusModelAction.newModelUpdate(self)
                            .jobMaybeUpdate(entityHolder -> Optional.of(entityHolder.addTag(ATTR_JOB_CLOSED, true)));

                    return store.deleteJob(job).andThen(Observable.just(ModelActionHolder.reference(modelUpdateAction)));
                });
    }

    public static boolean isClosed(EntityHolder model) {
        return (Boolean) model.getAttributes().getOrDefault(ATTR_JOB_CLOSED, Boolean.FALSE);
    }
}
