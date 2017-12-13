package io.netflix.titus.master.jobmanager.service.service.action;

import io.netflix.titus.api.jobmanager.model.job.Capacity;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobFunctions;
import io.netflix.titus.api.jobmanager.model.job.ServiceJobProcesses;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.api.jobmanager.store.JobStore;
import io.netflix.titus.common.framework.reconciler.ModelActionHolder;
import io.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import io.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.master.jobmanager.service.common.action.TitusModelAction;
import io.netflix.titus.master.jobmanager.service.event.JobManagerReconcilerEvent;
import rx.Observable;

public class BasicServiceJobActions {

    /**
     * Update job capacity.
     */
    public static TitusChangeAction updateJobCapacityAction(ReconciliationEngine<JobManagerReconcilerEvent> engine, Capacity capacity, JobStore jobStore) {
        return TitusChangeAction.newAction("updateJobCapacityAction")
                .id(engine.getReferenceView().getId())
                .trigger(V3JobOperations.Trigger.API)
                .summary("Changing job capacity to: %s", capacity)
                .changeWithModelUpdates(self -> {
                    Job<ServiceJobExt> serviceJob = engine.getReferenceView().getEntity();
                    Job<ServiceJobExt> updatedJob = JobFunctions.changeServiceJobCapacity(serviceJob, capacity);

                    TitusModelAction modelAction = TitusModelAction.newModelUpdate(self).jobUpdate(jobHolder -> jobHolder.setEntity(updatedJob));

                    return jobStore.updateJob(updatedJob).andThen(Observable.just(ModelActionHolder.referenceAndStore(modelAction)));
                });
    }

    /**
     * Change job 'enable' status.
     */
    public static TitusChangeAction updateJobEnableStatus(ReconciliationEngine<JobManagerReconcilerEvent> engine, boolean enabled, JobStore jobStore) {
        return TitusChangeAction.newAction("updateJobCapacityAction")
                .id(engine.getReferenceView().getId())
                .trigger(V3JobOperations.Trigger.API)
                .summary("Changing job enable status to: %s", enabled)
                .changeWithModelUpdates(self -> {
                    Job<ServiceJobExt> serviceJob = engine.getReferenceView().getEntity();
                    Job<ServiceJobExt> updatedJob = JobFunctions.changeJobEnabledStatus(serviceJob, enabled);

                    TitusModelAction modelAction = TitusModelAction.newModelUpdate(self).jobUpdate(jobHolder -> jobHolder.setEntity(updatedJob));

                    return jobStore.updateJob(updatedJob).andThen(Observable.just(ModelActionHolder.referenceAndStore(modelAction)));
                });
    }

    /**
     * Change job service processes configuration.
     */
    public static TitusChangeAction updateServiceJobProcesses(ReconciliationEngine<JobManagerReconcilerEvent> engine, ServiceJobProcesses processes, JobStore jobStore) {
        return TitusChangeAction.newAction("updateServiceJobProcesses")
                .id(engine.getReferenceView().getId())
                .trigger(V3JobOperations.Trigger.API)
                .summary("Changing job service processes to: %s", processes)
                .changeWithModelUpdates(self -> {
                    Job<ServiceJobExt> serviceJob = engine.getReferenceView().getEntity();
                    Job<ServiceJobExt> updatedJob = JobFunctions.changeServiceJobProcesses(serviceJob, processes);

                    TitusModelAction modelAction = TitusModelAction.newModelUpdate(self).jobUpdate(jobHolder -> jobHolder.setEntity(updatedJob));

                    return jobStore.updateJob(updatedJob).andThen(Observable.just(ModelActionHolder.referenceAndStore(modelAction)));
                });
    }
}
