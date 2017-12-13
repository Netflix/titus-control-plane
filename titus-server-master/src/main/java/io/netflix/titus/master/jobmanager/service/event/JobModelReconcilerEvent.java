package io.netflix.titus.master.jobmanager.service.event;

import java.util.Optional;

import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.common.framework.reconciler.EntityHolder;
import io.netflix.titus.common.framework.reconciler.ModelActionHolder;
import io.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;

public abstract class JobModelReconcilerEvent extends JobManagerReconcilerEvent {

    protected JobModelReconcilerEvent(Job<?> job, long transactionId) {
        super(job, transactionId);
    }

    public static class JobNewModelReconcilerEvent extends JobModelReconcilerEvent {
        private final EntityHolder newRoot;

        public JobNewModelReconcilerEvent(EntityHolder newRoot) {
            super(newRoot.getEntity(), -1);
            this.newRoot = newRoot;
        }

        public EntityHolder getNewRoot() {
            return newRoot;
        }
    }

    public static class JobModelUpdateReconcilerEvent extends JobModelReconcilerEvent {

        private final TitusChangeAction changeAction;
        private final ModelActionHolder modelActionHolder;
        private final EntityHolder changedEntityHolder;
        private final Optional<EntityHolder> previousEntityHolder;

        public JobModelUpdateReconcilerEvent(Job<?> job,
                                             TitusChangeAction changeAction,
                                             ModelActionHolder modelActionHolder,
                                             EntityHolder changedEntityHolder,
                                             Optional<EntityHolder> previousEntityHolder,
                                             long transactionId) {
            super(job, transactionId);
            this.changeAction = changeAction;
            this.modelActionHolder = modelActionHolder;
            this.changedEntityHolder = changedEntityHolder;
            this.previousEntityHolder = previousEntityHolder;
        }

        public TitusChangeAction getChangeAction() {
            return changeAction;
        }

        public ModelActionHolder getModelActionHolder() {
            return modelActionHolder;
        }

        public EntityHolder getChangedEntityHolder() {
            return changedEntityHolder;
        }

        public Optional<EntityHolder> getPreviousEntityHolder() {
            return previousEntityHolder;
        }
    }

    public static class JobModelUpdateErrorReconcilerEvent extends JobModelReconcilerEvent {

        private final TitusChangeAction changeAction;
        private final ModelActionHolder modelActionHolder;
        private final EntityHolder previousEntityHolder;
        private final Throwable error;

        public JobModelUpdateErrorReconcilerEvent(Job<?> job,
                                                  TitusChangeAction changeAction,
                                                  ModelActionHolder modelActionHolder,
                                                  EntityHolder previousEntityHolder,
                                                  Throwable error,
                                                  long transactionId) {
            super(job, transactionId);
            this.changeAction = changeAction;
            this.modelActionHolder = modelActionHolder;
            this.previousEntityHolder = previousEntityHolder;
            this.error = error;
        }

        public TitusChangeAction getChangeAction() {
            return changeAction;
        }

        public ModelActionHolder getModelActionHolder() {
            return modelActionHolder;
        }

        public EntityHolder getPreviousEntityHolder() {
            return previousEntityHolder;
        }

        public Throwable getError() {
            return error;
        }
    }
}
