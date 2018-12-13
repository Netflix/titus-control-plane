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

package com.netflix.titus.common.framework.reconciler.internal;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FailedTransaction<EVENT> implements Transaction {

    private static final Logger logger = LoggerFactory.getLogger(FailedTransaction.class);

    enum TransactionStep {
        ChangeActionFailed,
        ErrorEventsEmitted,
        SubscribersCompleted
    }

    private final DefaultReconciliationEngine<EVENT> engine;
    private final ChangeActionHolder changeActionHolder;
    private final Throwable error;

    private TransactionStep transactionStep = TransactionStep.ChangeActionFailed;

    FailedTransaction(DefaultReconciliationEngine<EVENT> engine, ChangeActionHolder changeActionHolder, Throwable error) {
        this.engine = engine;
        this.changeActionHolder = changeActionHolder;
        this.error = error;
    }

    @Override
    public void close() {
        changeActionHolder.getSubscriber().unsubscribe();
        this.transactionStep = TransactionStep.SubscribersCompleted;
    }

    @Override
    public boolean isClosed() {
        return transactionStep == TransactionStep.SubscribersCompleted;
    }

    @Override
    public Optional<ModelHolder> applyModelUpdates(ModelHolder modelHolder) {
        return Optional.empty();
    }

    @Override
    public void emitEvents() {
        if (transactionStep == TransactionStep.ChangeActionFailed) {
            this.transactionStep = TransactionStep.ErrorEventsEmitted;

            long now = engine.getTitusRuntime().getClock().wallTime();
            EVENT event = engine.getEventFactory().newChangeErrorEvent(engine, changeActionHolder.getChangeAction(), error, now - changeActionHolder.getCreateTimestamp(), 0, changeActionHolder.getTransactionId());

            engine.emitEvent(event);
        }
    }

    @Override
    public boolean completeSubscribers() {
        if (transactionStep == TransactionStep.ErrorEventsEmitted) {
            this.transactionStep = TransactionStep.SubscribersCompleted;

            try {
                changeActionHolder.getSubscriber().onError(error);
            } catch (Exception e) {
                logger.warn("Client subscriber onError handler thrown an exception: rootId={}, error={}", engine.getRunningView().getId(), e.getMessage());
            }
            return true;
        }
        return false;
    }
}
