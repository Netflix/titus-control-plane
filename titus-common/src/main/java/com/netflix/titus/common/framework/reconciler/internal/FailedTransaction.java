package com.netflix.titus.common.framework.reconciler.internal;

import java.util.Optional;

class FailedTransaction<EVENT> extends Transaction {

    enum TransactionStep {
        ChangeActionFailed,
        ErrorEventsEmitted,
        SubscribersCompleted
    }

    private final DefaultReconciliationEngine<EVENT> engine;
    private final ChangeActionHolder changeActionHolder;
    private final Throwable error;

    private TransactionStep transactionStep = TransactionStep.ChangeActionFailed;

    public FailedTransaction(DefaultReconciliationEngine<EVENT> engine, ChangeActionHolder changeActionHolder, Throwable error) {
        this.engine = engine;
        this.changeActionHolder = changeActionHolder;
        this.error = error;
    }

    @Override
    void close() {
        changeActionHolder.getSubscriber().unsubscribe();
        this.transactionStep = TransactionStep.SubscribersCompleted;
    }

    @Override
    boolean isClosed() {
        return transactionStep == TransactionStep.SubscribersCompleted;
    }

    @Override
    Optional<ModelHolder> applyModelUpdates(ModelHolder modelHolder) {
        return Optional.empty();
    }

    @Override
    void emitEvents() {
        if (transactionStep == TransactionStep.ChangeActionFailed) {
            this.transactionStep = TransactionStep.ErrorEventsEmitted;

            long now = engine.getTitusRuntime().getClock().wallTime();
            EVENT event = engine.getEventFactory().newChangeErrorEvent(engine, changeActionHolder.getChangeAction(), error, now - changeActionHolder.getCreateTimestamp(), 0, changeActionHolder.getTransactionId());

            engine.emitEvent(event);
        }
    }

    @Override
    boolean completeSubscribers() {
        if (transactionStep == TransactionStep.ErrorEventsEmitted) {
            this.transactionStep = TransactionStep.SubscribersCompleted;

            changeActionHolder.getSubscriber().onError(error);
            return true;
        }
        return false;
    }
}
