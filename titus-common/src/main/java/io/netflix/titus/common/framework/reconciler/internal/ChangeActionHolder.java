package io.netflix.titus.common.framework.reconciler.internal;

import io.netflix.titus.common.framework.reconciler.ChangeAction;

final class ChangeActionHolder {

    private final ChangeAction changeAction;
    private final long transactionId;
    private final long createTimeMs;

    ChangeActionHolder(ChangeAction changeAction, long transactionId, long createTimeMs) {
        this.changeAction = changeAction;
        this.transactionId = transactionId;
        this.createTimeMs = createTimeMs;
    }

    ChangeAction getChangeAction() {
        return changeAction;
    }

    long getTransactionId() {
        return transactionId;
    }

    long getCreateTimeMs() {
        return createTimeMs;
    }
}
