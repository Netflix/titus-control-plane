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

package com.netflix.titus.runtime.connector.agent.replicator;

import java.time.Duration;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import com.netflix.titus.api.agent.model.event.AgentEvent;
import com.netflix.titus.api.agent.model.event.AgentSnapshotEndEvent;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.runtime.connector.agent.AgentDataReplicator;
import com.netflix.titus.runtime.connector.agent.AgentManagementClient;
import com.netflix.titus.runtime.connector.agent.AgentSnapshot;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicator;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicatorDelegate;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicatorMetrics;
import com.netflix.titus.runtime.connector.common.replicator.ReplicatorEvent;
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
        StreamDataReplicator<AgentSnapshot, AgentEvent> original = StreamDataReplicator.newStreamDataReplicator(
                newReplicatorEventStream(client, titusRuntime),
                new AgentDataReplicatorMetrics(AGENT_REPLICATOR, titusRuntime),
                titusRuntime
        ).blockFirst(Duration.ofMillis(AGENT_BOOTSTRAP_TIMEOUT_MS));

        this.replicator = new AgentDataReplicatorImpl(original);
    }

    @PreDestroy
    public void shutdown() {
        ExceptionExt.silent(replicator::close);
    }

    @Override
    public AgentDataReplicator get() {
        return replicator;
    }

    private static RetryableReplicatorEventStream<AgentSnapshot, AgentEvent> newReplicatorEventStream(AgentManagementClient client, TitusRuntime titusRuntime) {
        GrpcAgentReplicatorEventStream grpcEventStream = new GrpcAgentReplicatorEventStream(
                client,
                new AgentDataReplicatorMetrics(AGENT_REPLICATOR_GRPC_STREAM, titusRuntime),
                titusRuntime,
                Schedulers.parallel()
        );

        return new RetryableReplicatorEventStream<>(
                AgentSnapshot.empty(),
                AgentSnapshotEndEvent.snapshotEnd(),
                grpcEventStream,
                new AgentDataReplicatorMetrics(AGENT_REPLICATOR_RETRYABLE_STREAM, titusRuntime),
                titusRuntime,
                Schedulers.parallel()
        );
    }

    private static class AgentDataReplicatorImpl extends DataReplicatorDelegate<AgentSnapshot, AgentEvent> implements AgentDataReplicator {
        AgentDataReplicatorImpl(DataReplicator<AgentSnapshot, AgentEvent> delegate) {
            super(delegate);
        }
    }

    private static class AgentDataReplicatorMetrics extends DataReplicatorMetrics<AgentSnapshot, AgentEvent> {

        private AgentDataReplicatorMetrics(String source, TitusRuntime titusRuntime) {
            super(source, titusRuntime);
        }

        @Override
        public void event(ReplicatorEvent<AgentSnapshot, AgentEvent> event) {
            super.event(event);
            setCacheCollectionSize("instanceGroups", event.getSnapshot().getInstanceGroups().size());
            setCacheCollectionSize("instances", event.getSnapshot().getInstances().size());
        }
    }
}
