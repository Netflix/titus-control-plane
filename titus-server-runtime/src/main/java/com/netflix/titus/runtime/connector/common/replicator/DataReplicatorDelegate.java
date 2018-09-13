package com.netflix.titus.runtime.connector.common.replicator;

import reactor.core.publisher.Flux;

public class DataReplicatorDelegate<D> implements DataReplicator<D> {

    private DataReplicator<D> delegate;

    public DataReplicatorDelegate(DataReplicator<D> delegate) {
        this.delegate = delegate;
    }

    @Override
    public D getCurrent() {
        return delegate.getCurrent();
    }

    @Override
    public long getStalenessMs() {
        return delegate.getStalenessMs();
    }

    @Override
    public Flux<Long> observeDataStalenessMs() {
        return delegate.observeDataStalenessMs();
    }
}
