package com.netflix.titus.common.framework.reconciler.internal;

import java.util.List;
import java.util.Optional;

class CompositeTransaction extends Transaction {

    private final List<Transaction> transactions;

    public CompositeTransaction(List<Transaction> transactions) {
        this.transactions = transactions;
    }

    @Override
    void close() {
        transactions.forEach(Transaction::close);
    }

    @Override
    boolean isClosed() {
        for (Transaction transaction : transactions) {
            if (!transaction.isClosed()) {
                return false;
            }
        }
        return true;
    }

    @Override
    Optional<ModelHolder> applyModelUpdates(ModelHolder modelHolder) {
        ModelHolder currentModelHolder = modelHolder;
        for (Transaction transaction : transactions) {
            currentModelHolder = transaction.applyModelUpdates(currentModelHolder).orElse(currentModelHolder);
        }
        return currentModelHolder == modelHolder ? Optional.empty() : Optional.of(currentModelHolder);
    }

    @Override
    void emitEvents() {
        transactions.forEach(Transaction::emitEvents);
    }

    @Override
    boolean completeSubscribers() {
        boolean anyChange = false;
        for (Transaction transaction : transactions) {
            anyChange = transaction.completeSubscribers() | anyChange;
        }
        return anyChange;
    }
}
