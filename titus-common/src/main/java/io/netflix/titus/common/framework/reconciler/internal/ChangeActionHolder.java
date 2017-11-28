package io.netflix.titus.common.framework.reconciler.internal;

import io.netflix.titus.common.framework.reconciler.ChangeAction;

final class ChangeActionHolder<CHANGE> {

    private final ChangeAction<CHANGE> changeAction;
    private final long transactionId;

    ChangeActionHolder(ChangeAction<CHANGE> changeAction, long transactionId) {
        this.changeAction = changeAction;
        this.transactionId = transactionId;
    }

    ChangeAction<CHANGE> getChangeAction() {
        return changeAction;
    }

    long getTransactionId() {
        return transactionId;
    }
}
