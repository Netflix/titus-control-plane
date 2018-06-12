package com.netflix.titus.runtime.connector.agent.replicator;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.runtime.connector.agent.AgentDataReplicator;
import com.netflix.titus.runtime.connector.agent.AgentManagementClient;
import com.netflix.titus.runtime.connector.agent.AgentSnapshot;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicatorMetrics;
import com.netflix.titus.runtime.connector.common.replicator.RetryableReplicatorEventStream;
import com.netflix.titus.runtime.connector.common.replicator.StreamDataReplicator;
import rx.schedulers.Schedulers;

@Singleton
public class DefaultAgentDataReplicator extends StreamDataReplicator<AgentSnapshot> implements AgentDataReplicator {

    private static final String AGENT_REPLICATOR = "agentReplicator";
    private static final String AGENT_REPLICATOR_RETRYABLE_STREAM = "agentReplicatorRetryableStream";
    private static final String AGENT_REPLICATOR_GRPC_STREAM = "agentReplicatorGrpcStream";

    @Inject
    public DefaultAgentDataReplicator(AgentManagementClient client, TitusRuntime titusRuntime) {
        super(
                newReplicatorEventStream(client, titusRuntime),
                new DataReplicatorMetrics(AGENT_REPLICATOR, titusRuntime),
                titusRuntime
        );
    }

    private static RetryableReplicatorEventStream<AgentSnapshot> newReplicatorEventStream(AgentManagementClient client, TitusRuntime titusRuntime) {
        GrpcAgentReplicatorEventStream grpcEventStream = new GrpcAgentReplicatorEventStream(
                client,
                new DataReplicatorMetrics(AGENT_REPLICATOR_GRPC_STREAM, titusRuntime),
                titusRuntime,
                Schedulers.computation()
        );

        return new RetryableReplicatorEventStream<>(
                grpcEventStream,
                new DataReplicatorMetrics(AGENT_REPLICATOR_RETRYABLE_STREAM, titusRuntime),
                titusRuntime,
                Schedulers.computation()
        );
    }
}
