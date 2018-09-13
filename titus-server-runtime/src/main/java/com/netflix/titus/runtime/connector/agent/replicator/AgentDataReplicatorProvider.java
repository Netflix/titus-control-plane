package com.netflix.titus.runtime.connector.agent.replicator;

import java.time.Duration;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.runtime.connector.agent.AgentDataReplicator;
import com.netflix.titus.runtime.connector.agent.AgentManagementClient;
import com.netflix.titus.runtime.connector.agent.AgentSnapshot;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicator;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicatorDelegate;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicatorMetrics;
import com.netflix.titus.runtime.connector.common.replicator.RetryableReplicatorEventStream;
import com.netflix.titus.runtime.connector.common.replicator.StreamDataReplicator;
import reactor.core.scheduler.Schedulers;

@Singleton
public class AgentDataReplicatorProvider implements Provider<AgentDataReplicator> {

    private static final String AGENT_REPLICATOR = "agentReplicator";
    private static final String AGENT_REPLICATOR_RETRYABLE_STREAM = "agentReplicatorRetryableStream";
    private static final String AGENT_REPLICATOR_GRPC_STREAM = "agentReplicatorGrpcStream";

    private static final long AGENT_BOOTSTRAP_TIMEOUT_MS = 120_000;

    private final AgentDataReplicator replicator;

    @Inject
    public AgentDataReplicatorProvider(AgentManagementClient client, TitusRuntime titusRuntime) {
        StreamDataReplicator<AgentSnapshot> original = StreamDataReplicator.newStreamDataReplicator(
                newReplicatorEventStream(client, titusRuntime),
                new DataReplicatorMetrics(AGENT_REPLICATOR, titusRuntime),
                titusRuntime
        ).blockFirst(Duration.ofMillis(AGENT_BOOTSTRAP_TIMEOUT_MS));

        this.replicator = new AgentDataReplicatorImpl(original);
    }

    @Override
    public AgentDataReplicator get() {
        return replicator;
    }

    private static RetryableReplicatorEventStream<AgentSnapshot> newReplicatorEventStream(AgentManagementClient client, TitusRuntime titusRuntime) {
        GrpcAgentReplicatorEventStream grpcEventStream = new GrpcAgentReplicatorEventStream(
                client,
                new DataReplicatorMetrics(AGENT_REPLICATOR_GRPC_STREAM, titusRuntime),
                titusRuntime,
                Schedulers.parallel()
        );

        return new RetryableReplicatorEventStream<>(
                grpcEventStream,
                new DataReplicatorMetrics(AGENT_REPLICATOR_RETRYABLE_STREAM, titusRuntime),
                titusRuntime,
                Schedulers.parallel()
        );
    }

    private static class AgentDataReplicatorImpl extends DataReplicatorDelegate<AgentSnapshot> implements AgentDataReplicator {
        AgentDataReplicatorImpl(DataReplicator<AgentSnapshot> delegate) {
            super(delegate);
        }
    }
}
