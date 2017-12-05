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

package io.netflix.titus.master.jobmanager.service.event;

import java.util.Optional;

import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.common.framework.reconciler.ChangeAction;
import io.netflix.titus.common.framework.reconciler.EntityHolder;
import io.netflix.titus.common.framework.reconciler.ModelActionHolder;
import io.netflix.titus.common.framework.reconciler.ReconcileEventFactory;
import io.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import io.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.master.jobmanager.service.event.JobChangeReconcilerEvent.JobAfterChangeReconcilerEvent;
import io.netflix.titus.master.jobmanager.service.event.JobChangeReconcilerEvent.JobBeforeChangeReconcilerEvent;
import io.netflix.titus.master.jobmanager.service.event.JobChangeReconcilerEvent.JobChangeErrorReconcilerEvent;
import io.netflix.titus.master.jobmanager.service.event.JobModelReconcilerEvent.JobModelUpdateErrorReconcilerEvent;
import io.netflix.titus.master.jobmanager.service.event.JobModelReconcilerEvent.JobModelUpdateReconcilerEvent;
import io.netflix.titus.master.jobmanager.service.event.JobModelReconcilerEvent.JobNewModelReconcilerEvent;

/**
 */
public class JobEventFactory implements ReconcileEventFactory<JobManagerReconcilerEvent> {

    @Override
    public JobManagerReconcilerEvent newBeforeChangeEvent(ReconciliationEngine<JobManagerReconcilerEvent> engine,
                                                          ChangeAction changeAction,
                                                          long transactionId) {
        return new JobBeforeChangeReconcilerEvent(engine.getReferenceView().getEntity(), (TitusChangeAction) changeAction, transactionId);
    }

    @Override
    public JobManagerReconcilerEvent newAfterChangeEvent(ReconciliationEngine<JobManagerReconcilerEvent> engine,
                                                         ChangeAction changeAction,
                                                         long executionTimeMs,
                                                         long transactionId) {
        return new JobAfterChangeReconcilerEvent(engine.getReferenceView().getEntity(), (TitusChangeAction) changeAction, executionTimeMs, transactionId);
    }

    @Override
    public JobManagerReconcilerEvent newChangeErrorEvent(ReconciliationEngine<JobManagerReconcilerEvent> engine,
                                                         ChangeAction changeAction,
                                                         Throwable error,
                                                         long executionTimeMs,
                                                         long transactionId) {
        return new JobChangeErrorReconcilerEvent(engine.getReferenceView().getEntity(), (TitusChangeAction) changeAction, error, executionTimeMs, transactionId);
    }

    @Override
    public JobManagerReconcilerEvent newModelEvent(ReconciliationEngine<JobManagerReconcilerEvent> engine,
                                                   EntityHolder newRoot) {
        return new JobNewModelReconcilerEvent(newRoot);
    }

    @Override
    public JobManagerReconcilerEvent newModelUpdateEvent(ReconciliationEngine<JobManagerReconcilerEvent> engine,
                                                         ChangeAction changeAction,
                                                         ModelActionHolder modelActionHolder,
                                                         EntityHolder changedEntityHolder,
                                                         Optional<EntityHolder> previousEntityHolder,
                                                         long transactionId) {
        return new JobModelUpdateReconcilerEvent(getJob(engine, modelActionHolder), (TitusChangeAction) changeAction, modelActionHolder, changedEntityHolder, previousEntityHolder, transactionId);
    }

    @Override
    public JobManagerReconcilerEvent newModelUpdateErrorEvent(ReconciliationEngine<JobManagerReconcilerEvent> engine,
                                                              ChangeAction changeAction,
                                                              ModelActionHolder modelActionHolder,
                                                              EntityHolder previousEntityHolder,
                                                              Throwable error,
                                                              long transactionId) {
        return new JobModelUpdateErrorReconcilerEvent(getJob(engine, modelActionHolder), (TitusChangeAction) changeAction, modelActionHolder, previousEntityHolder, error, transactionId);
    }

    private Job<?> getJob(ReconciliationEngine<JobManagerReconcilerEvent> engine, ModelActionHolder modelActionHolder) {
        switch (modelActionHolder.getModel()) {
            case Running:
                return engine.getRunningView().getEntity();
            case Store:
                return engine.getStoreView().getEntity();
            case Reference:
            default:
                return engine.getReferenceView().getEntity();
        }
    }
}
