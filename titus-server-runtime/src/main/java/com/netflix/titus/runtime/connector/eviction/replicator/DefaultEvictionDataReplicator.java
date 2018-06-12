package com.netflix.titus.runtime.connector.eviction.replicator;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicatorMetrics;
import com.netflix.titus.runtime.connector.common.replicator.RetryableReplicatorEventStream;
import com.netflix.titus.runtime.connector.common.replicator.StreamDataReplicator;
import com.netflix.titus.runtime.connector.eviction.EvictionDataReplicator;
import com.netflix.titus.runtime.connector.eviction.EvictionDataSnapshot;
import com.netflix.titus.runtime.connector.eviction.EvictionServiceClient;
import rx.schedulers.Schedulers;

@Singleton
public class DefaultEvictionDataReplicator extends StreamDataReplicator<EvictionDataSnapshot> implements EvictionDataReplicator {

    private static final String EVICTION_REPLICATOR = "evictionReplicator";
    private static final String EVICTION_REPLICATOR_RETRYABLE_STREAM = "evictionReplicatorRetryableStream";
    private static final String EVICTION_REPLICATOR_GRPC_STREAM = "evictionReplicatorGrpcStream";

    @Inject
    public DefaultEvictionDataReplicator(EvictionServiceClient client, TitusRuntime titusRuntime) {
        super(
                newReplicatorEventStream(client, titusRuntime),
                new DataReplicatorMetrics(EVICTION_REPLICATOR, titusRuntime),
                titusRuntime
        );
    }

    private static RetryableReplicatorEventStream<EvictionDataSnapshot> newReplicatorEventStream(EvictionServiceClient client, TitusRuntime titusRuntime) {
        GrpcEvictionReplicatorEventStream grpcEventStream = new GrpcEvictionReplicatorEventStream(
                client,
                new DataReplicatorMetrics(EVICTION_REPLICATOR_GRPC_STREAM, titusRuntime),
                titusRuntime,
                Schedulers.computation()
        );

        return new RetryableReplicatorEventStream<>(
                grpcEventStream,
                new DataReplicatorMetrics(EVICTION_REPLICATOR_RETRYABLE_STREAM, titusRuntime),
                titusRuntime,
                Schedulers.computation()
        );
    }
}
