package com.netflix.titus.common.framework.reconciler.internal;

import java.util.Optional;

interface Transaction {

    void close();

    boolean isClosed();

    Optional<ModelHolder> applyModelUpdates(ModelHolder modelHolder);

    void emitEvents();

    boolean completeSubscribers();
}
