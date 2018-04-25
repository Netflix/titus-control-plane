package com.netflix.titus.common.framework.reconciler.internal;

import java.util.Optional;

class EmptyTransaction extends Transaction {

    static final Transaction EMPTY = new EmptyTransaction();

    @Override
    void close() {
    }

    @Override
    boolean isClosed() {
        return true;
    }

    @Override
    Optional<ModelHolder> applyModelUpdates(ModelHolder modelHolder) {
        return Optional.empty();
    }

    @Override
    void emitEvents() {
    }

    @Override
    boolean completeSubscribers() {
        return false;
    }
}
