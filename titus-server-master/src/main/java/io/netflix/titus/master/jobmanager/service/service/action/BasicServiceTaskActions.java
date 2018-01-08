package io.netflix.titus.master.jobmanager.service.service.action;

import io.netflix.titus.api.jobmanager.model.job.ServiceJobTask;
import io.netflix.titus.api.jobmanager.service.V3JobOperations.Trigger;
import io.netflix.titus.api.jobmanager.store.JobStore;
import io.netflix.titus.common.framework.reconciler.ModelActionHolder;
import io.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.master.jobmanager.service.common.action.TitusModelAction;
import rx.Observable;

public final class BasicServiceTaskActions {

    /**
     * Remove completed task from reconciler.
     */
    public static TitusChangeAction removeFinishedServiceTaskAction(JobStore jobStore, ServiceJobTask task) {
        return TitusChangeAction.newAction("removeFinishedServiceTaskAction")
                .task(task)
                .trigger(Trigger.Reconciler)
                .summary("Removing completed task from reconciler")
                .changeWithModelUpdates(self ->
                        jobStore.deleteTask(task).andThen(
                                Observable.just(ModelActionHolder.allModels(TitusModelAction.newModelUpdate(self).removeTask(task)))
                        ));
    }
}
