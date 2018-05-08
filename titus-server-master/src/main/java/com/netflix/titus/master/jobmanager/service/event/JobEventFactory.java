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

package com.netflix.titus.master.jobmanager.service.event;

import java.util.Optional;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.common.framework.reconciler.ChangeAction;
import com.netflix.titus.common.framework.reconciler.EntityHolder;
import com.netflix.titus.common.framework.reconciler.ModelActionHolder;
import com.netflix.titus.common.framework.reconciler.ReconcileEventFactory;
import com.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import com.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import com.netflix.titus.master.jobmanager.service.event.JobChangeReconcilerEvent.JobAfterChangeReconcilerEvent;
import com.netflix.titus.master.jobmanager.service.event.JobChangeReconcilerEvent.JobBeforeChangeReconcilerEvent;
import com.netflix.titus.master.jobmanager.service.event.JobChangeReconcilerEvent.JobChangeErrorReconcilerEvent;

/**
 */
public class JobEventFactory implements ReconcileEventFactory<JobManagerReconcilerEvent> {

    @Override
    public JobManagerReconcilerEvent newBeforeChangeEvent(ReconciliationEngine<JobManagerReconcilerEvent> engine,
                                                          ChangeAction changeAction,
                                                          String transactionId) {
        return new JobBeforeChangeReconcilerEvent(engine.getReferenceView().getEntity(), (TitusChangeAction) changeAction, transactionId);
    }

    @Override
    public JobManagerReconcilerEvent newAfterChangeEvent(ReconciliationEngine<JobManagerReconcilerEvent> engine,
                                                         ChangeAction changeAction,
                                                         long waitTimeMs,
                                                         long executionTimeMs,
                                                         String transactionId) {
        return new JobAfterChangeReconcilerEvent(engine.getReferenceView().getEntity(), (TitusChangeAction) changeAction, waitTimeMs, executionTimeMs, transactionId);
    }

    @Override
    public JobManagerReconcilerEvent newChangeErrorEvent(ReconciliationEngine<JobManagerReconcilerEvent> engine,
                                                         ChangeAction changeAction,
                                                         Throwable error,
                                                         long waitTimeMs,
                                                         long executionTimeMs,
                                                         String transactionId) {
        return new JobChangeErrorReconcilerEvent(engine.getReferenceView().getEntity(), (TitusChangeAction) changeAction, error, waitTimeMs, executionTimeMs, transactionId);
    }

    @Override
    public JobManagerReconcilerEvent newModelEvent(ReconciliationEngine<JobManagerReconcilerEvent> engine,
                                                   EntityHolder newRoot) {
        return new JobModelReconcilerEvent.JobNewModelReconcilerEvent(newRoot);
    }

    @Override
    public JobManagerReconcilerEvent newModelUpdateEvent(ReconciliationEngine<JobManagerReconcilerEvent> engine,
                                                         ChangeAction changeAction,
                                                         ModelActionHolder modelActionHolder,
                                                         EntityHolder changedEntityHolder,
                                                         Optional<EntityHolder> previousEntityHolder,
                                                         String transactionId) {
        return new JobModelReconcilerEvent.JobModelUpdateReconcilerEvent(getJob(engine, modelActionHolder), (TitusChangeAction) changeAction, modelActionHolder, changedEntityHolder, previousEntityHolder, transactionId);
    }

    @Override
    public JobManagerReconcilerEvent newModelUpdateErrorEvent(ReconciliationEngine<JobManagerReconcilerEvent> engine,
                                                              ChangeAction changeAction,
                                                              ModelActionHolder modelActionHolder,
                                                              EntityHolder previousEntityHolder,
                                                              Throwable error,
                                                              String transactionId) {
        return new JobModelReconcilerEvent.JobModelUpdateErrorReconcilerEvent(getJob(engine, modelActionHolder), (TitusChangeAction) changeAction, modelActionHolder, previousEntityHolder, error, transactionId);
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
