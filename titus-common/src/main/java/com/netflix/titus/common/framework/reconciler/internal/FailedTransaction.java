package com.netflix.titus.common.framework.reconciler.internal;

import java.util.Optional;

class FailedTransaction<EVENT> implements Transaction {

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

            changeActionHolder.getSubscriber().onError(error);
            return true;
        }
        return false;
    }
}
