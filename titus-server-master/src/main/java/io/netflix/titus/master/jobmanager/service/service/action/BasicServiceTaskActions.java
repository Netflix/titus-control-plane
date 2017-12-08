package io.netflix.titus.master.jobmanager.service.service.action;

import io.netflix.titus.api.jobmanager.model.job.ServiceJobTask;
import io.netflix.titus.api.jobmanager.service.V3JobOperations.Trigger;
import io.netflix.titus.common.framework.reconciler.ModelActionHolder;
import io.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.master.jobmanager.service.common.action.TitusModelAction;

public final class BasicServiceTaskActions {

    /**
     * Remove completed task from reconciler.
     */
    public static TitusChangeAction removeFinishedServiceTaskAction(ServiceJobTask task) {
        return TitusChangeAction.newAction("removeFinishedServiceTaskAction")
                .task(task)
                .trigger(Trigger.Reconciler)
                .summary("Removing completed task from reconciler")
                .applyModelUpdates(self -> ModelActionHolder.allModels(TitusModelAction.newModelUpdate(self).removeTask(task)));
    }
}
