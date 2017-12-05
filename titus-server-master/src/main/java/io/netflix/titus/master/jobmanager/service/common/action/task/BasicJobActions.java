package io.netflix.titus.master.jobmanager.service.common.action.task;

import java.util.Optional;

import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobFunctions;
import io.netflix.titus.api.jobmanager.model.job.JobState;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.api.jobmanager.store.JobStore;
import io.netflix.titus.common.framework.reconciler.EntityHolder;
import io.netflix.titus.common.framework.reconciler.ModelActionHolder;
import io.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import io.netflix.titus.master.jobmanager.service.common.action.JobChange;
import io.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.master.jobmanager.service.common.action.TitusModelAction;
import io.netflix.titus.master.jobmanager.service.event.JobManagerReconcilerEvent;
import rx.Observable;

public class BasicJobActions {

    public static final String ATTR_JOB_CLOSED = "job.closed";

    /**
     * Write an updated job record to a store. This command calls {@link JobStore#updateJob(Job)}, which assumes
     * that the job record was created already.
     */
    public static TitusChangeAction updateJobInStore(ReconciliationEngine<JobChange, JobManagerReconcilerEvent> engine, JobStore titusStore) {
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
