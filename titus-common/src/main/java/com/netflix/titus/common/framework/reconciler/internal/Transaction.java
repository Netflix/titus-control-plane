package com.netflix.titus.common.framework.reconciler.internal;

import java.util.Optional;

abstract class Transaction {

    abstract void close();

    abstract boolean isClosed();

    abstract Optional<ModelHolder> applyModelUpdates(ModelHolder modelHolder);

    abstract void emitEvents();

    abstract boolean completeSubscribers();

}
