/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
