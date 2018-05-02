package com.netflix.titus.common.framework.reconciler.internal;

import java.util.Optional;

class EmptyTransaction implements Transaction {

    static final Transaction EMPTY = new EmptyTransaction();

    @Override
    public void close() {
    }

    @Override
    public boolean isClosed() {
        return true;
    }

    @Override
    public Optional<ModelHolder> applyModelUpdates(ModelHolder modelHolder) {
        return Optional.empty();
    }

    @Override
    public void emitEvents() {
    }

    @Override
    public boolean completeSubscribers() {
        return false;
    }
}
