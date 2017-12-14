package io.netflix.titus.common.framework.reconciler.internal;

import io.netflix.titus.common.framework.reconciler.ChangeAction;

final class ChangeActionHolder {

    private final ChangeAction changeAction;
    private final long transactionId;

    ChangeActionHolder(ChangeAction changeAction, long transactionId) {
        this.changeAction = changeAction;
        this.transactionId = transactionId;
    }

    ChangeAction getChangeAction() {
        return changeAction;
    }

    long getTransactionId() {
        return transactionId;
    }
}
