package com.netflix.titus.common.framework.reconciler.internal;

import java.util.List;
import java.util.Optional;

class CompositeTransaction implements Transaction {

    private final List<Transaction> transactions;

    CompositeTransaction(List<Transaction> transactions) {
        this.transactions = transactions;
    }

    @Override
    public void close() {
        transactions.forEach(Transaction::close);
    }

    @Override
    public boolean isClosed() {
        for (Transaction transaction : transactions) {
            if (!transaction.isClosed()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Optional<ModelHolder> applyModelUpdates(ModelHolder modelHolder) {
        ModelHolder currentModelHolder = modelHolder;
        for (Transaction transaction : transactions) {
            currentModelHolder = transaction.applyModelUpdates(currentModelHolder).orElse(currentModelHolder);
        }
        return currentModelHolder == modelHolder ? Optional.empty() : Optional.of(currentModelHolder);
    }

    @Override
    public void emitEvents() {
        transactions.forEach(Transaction::emitEvents);
    }

    @Override
    public boolean completeSubscribers() {
        boolean anyChange = false;
        for (Transaction transaction : transactions) {
            anyChange = transaction.completeSubscribers() | anyChange;
        }
        return anyChange;
    }
}
