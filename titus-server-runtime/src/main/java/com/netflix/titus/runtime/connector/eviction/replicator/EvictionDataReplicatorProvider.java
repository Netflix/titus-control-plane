package com.netflix.titus.runtime.connector.eviction.replicator;

import java.time.Duration;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicator;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicatorDelegate;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicatorMetrics;
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
        StreamDataReplicator<EvictionDataSnapshot> original = StreamDataReplicator.newStreamDataReplicator(
                newReplicatorEventStream(client, titusRuntime),
                new DataReplicatorMetrics(EVICTION_REPLICATOR, titusRuntime),
                titusRuntime
        ).blockFirst(Duration.ofMillis(EVICTION_BOOTSTRAP_TIMEOUT_MS));

        this.replicator = new EvictionDataReplicatorImpl(original);
    }

    @Override
    public EvictionDataReplicator get() {
        return replicator;
    }

    private static RetryableReplicatorEventStream<EvictionDataSnapshot> newReplicatorEventStream(EvictionServiceClient client, TitusRuntime titusRuntime) {
        GrpcEvictionReplicatorEventStream grpcEventStream = new GrpcEvictionReplicatorEventStream(
                client,
                new DataReplicatorMetrics(EVICTION_REPLICATOR_GRPC_STREAM, titusRuntime),
                titusRuntime,
                Schedulers.parallel()
        );

        return new RetryableReplicatorEventStream<>(
                grpcEventStream,
                new DataReplicatorMetrics(EVICTION_REPLICATOR_RETRYABLE_STREAM, titusRuntime),
                titusRuntime,
                Schedulers.parallel()
        );
    }

    private static class EvictionDataReplicatorImpl extends DataReplicatorDelegate<EvictionDataSnapshot> implements EvictionDataReplicator {
        EvictionDataReplicatorImpl(DataReplicator<EvictionDataSnapshot> delegate) {
            super(delegate);
        }
    }
}
