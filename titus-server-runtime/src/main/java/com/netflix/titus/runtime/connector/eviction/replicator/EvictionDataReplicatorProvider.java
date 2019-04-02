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

package com.netflix.titus.runtime.connector.eviction.replicator;

import java.time.Duration;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import com.netflix.titus.api.eviction.model.event.EvictionEvent;
import com.netflix.titus.api.eviction.model.event.EvictionSnapshotEndEvent;
import com.netflix.titus.api.model.Level;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicator;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicatorDelegate;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicatorMetrics;
import com.netflix.titus.runtime.connector.common.replicator.ReplicatorEvent;
import com.netflix.titus.runtime.connector.common.replicator.RetryableReplicatorEventStream;
import com.netflix.titus.runtime.connector.common.replicator.StreamDataReplicator;
import com.netflix.titus.runtime.connector.eviction.EvictionDataReplicator;
import com.netflix.titus.runtime.connector.eviction.EvictionDataSnapshot;
import com.netflix.titus.runtime.connector.eviction.EvictionServiceClient;
import reactor.core.scheduler.Schedulers;

@Singleton
public class EvictionDataReplicatorProvider implements Provider<EvictionDataReplicator> {

    private static final String EVICTION_REPLICATOR = "evictionReplicator";
    private static final String EVICTION_REPLICATOR_RETRYABLE_STREAM = "evictionReplicatorRetryableStream";
    private static final String EVICTION_REPLICATOR_GRPC_STREAM = "evictionReplicatorGrpcStream";

    private static final long EVICTION_BOOTSTRAP_TIMEOUT_MS = 120_000;

    private final EvictionDataReplicatorImpl replicator;

    @Inject
    public EvictionDataReplicatorProvider(EvictionServiceClient client, TitusRuntime titusRuntime) {
        StreamDataReplicator<EvictionDataSnapshot, EvictionEvent> original = StreamDataReplicator.newStreamDataReplicator(
                newReplicatorEventStream(client, titusRuntime),
                new EvictionDataReplicatorMetrics(EVICTION_REPLICATOR, titusRuntime),
                titusRuntime
        ).blockFirst(Duration.ofMillis(EVICTION_BOOTSTRAP_TIMEOUT_MS));

        this.replicator = new EvictionDataReplicatorImpl(original);
    }

    @PreDestroy
    public void shutdown() {
        ExceptionExt.silent(replicator::close);
    }

    @Override
    public EvictionDataReplicator get() {
        return replicator;
    }

    private static RetryableReplicatorEventStream<EvictionDataSnapshot, EvictionEvent> newReplicatorEventStream(EvictionServiceClient client, TitusRuntime titusRuntime) {
        GrpcEvictionReplicatorEventStream grpcEventStream = new GrpcEvictionReplicatorEventStream(
                client,
                new EvictionDataReplicatorMetrics(EVICTION_REPLICATOR_GRPC_STREAM, titusRuntime),
                titusRuntime,
                Schedulers.parallel()
        );

        return new RetryableReplicatorEventStream<>(
                EvictionDataSnapshot.empty(),
                EvictionSnapshotEndEvent.getInstance(),
                grpcEventStream,
                new EvictionDataReplicatorMetrics(EVICTION_REPLICATOR_RETRYABLE_STREAM, titusRuntime),
                titusRuntime,
                Schedulers.parallel()
        );
    }

    private static class EvictionDataReplicatorImpl extends DataReplicatorDelegate<EvictionDataSnapshot, EvictionEvent> implements EvictionDataReplicator {
        EvictionDataReplicatorImpl(DataReplicator<EvictionDataSnapshot, EvictionEvent> delegate) {
            super(delegate);
        }
    }

    private static class EvictionDataReplicatorMetrics extends DataReplicatorMetrics<EvictionDataSnapshot, EvictionEvent> {

        private EvictionDataReplicatorMetrics(String source, TitusRuntime titusRuntime) {
            super(source, titusRuntime);
        }

        @Override
        public void event(ReplicatorEvent<EvictionDataSnapshot, EvictionEvent> event) {
            super.event(event);
            setCacheCollectionSize("capacityGroups", event.getSnapshot().getQuotas(Level.CapacityGroup).size());
            setCacheCollectionSize("jobs", event.getSnapshot().getQuotas(Level.Job).size());
        }
    }
}
