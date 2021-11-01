/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.runtime.connector.relocation.replicator;

import java.util.UUID;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import com.netflix.titus.api.relocation.model.event.TaskRelocationEvent;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicator;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicatorDelegate;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicatorMetrics;
import com.netflix.titus.runtime.connector.common.replicator.ReplicatorEvent;
import com.netflix.titus.runtime.connector.common.replicator.RetryableReplicatorEventStream;
import com.netflix.titus.runtime.connector.common.replicator.StreamDataReplicator;
import com.netflix.titus.runtime.connector.relocation.RelocationDataReplicator;
import com.netflix.titus.runtime.connector.relocation.RelocationServiceClient;
import com.netflix.titus.runtime.connector.relocation.TaskRelocationSnapshot;
import reactor.core.scheduler.Schedulers;

/**
 * {@link RelocationDataReplicator} cache unlike other caches is initialized lazily, and start empty.
 */
@Singleton
public class RelocationDataReplicatorProvider implements Provider<RelocationDataReplicator> {

    private static final String RELOCATION_REPLICATOR = "relocationReplicator";
    private static final String RELOCATION_REPLICATOR_RETRYABLE_STREAM = "relocationReplicatorRetryableStream";
    private static final String RELOCATION_REPLICATOR_GRPC_STREAM = "relocationReplicatorGrpcStream";

    /**
     * As we start with an empty cache, there is no event associated with the initial version of the snapshot.
     * Instead we use this constant value.
     */
    private static final TaskRelocationEvent STARTUP_EVENT = TaskRelocationEvent.taskRelocationPlanRemoved(UUID.randomUUID().toString());

    private final RelocationDataReplicatorImpl replicator;

    @Inject
    public RelocationDataReplicatorProvider(RelocationServiceClient client, TitusRuntime titusRuntime) {
        StreamDataReplicator<TaskRelocationSnapshot, TaskRelocationEvent> original = StreamDataReplicator.newStreamDataReplicator(
                new ReplicatorEvent<>(TaskRelocationSnapshot.empty(), STARTUP_EVENT, 0L),
                newReplicatorEventStream(client, titusRuntime),
                false,
                new RelocationDataReplicatorMetrics(RELOCATION_REPLICATOR, titusRuntime),
                titusRuntime
        );

        this.replicator = new RelocationDataReplicatorImpl(original);
    }

    @PreDestroy
    public void shutdown() {
        ExceptionExt.silent(replicator::close);
    }

    @Override
    public RelocationDataReplicatorImpl get() {
        return replicator;
    }

    private static RetryableReplicatorEventStream<TaskRelocationSnapshot, TaskRelocationEvent> newReplicatorEventStream(RelocationServiceClient client, TitusRuntime titusRuntime) {
        GrpcRelocationReplicatorEventStream grpcEventStream = new GrpcRelocationReplicatorEventStream(
                client,
                new DataReplicatorMetrics<>(RELOCATION_REPLICATOR_GRPC_STREAM, false, titusRuntime),
                titusRuntime,
                Schedulers.parallel()
        );

        return new RetryableReplicatorEventStream<>(
                grpcEventStream,
                new DataReplicatorMetrics<>(RELOCATION_REPLICATOR_RETRYABLE_STREAM, false, titusRuntime),
                titusRuntime,
                Schedulers.parallel()
        );
    }

    private static class RelocationDataReplicatorImpl extends DataReplicatorDelegate<TaskRelocationSnapshot, TaskRelocationEvent> implements RelocationDataReplicator {
        RelocationDataReplicatorImpl(DataReplicator<TaskRelocationSnapshot, TaskRelocationEvent> delegate) {
            super(delegate);
        }
    }

    private static class RelocationDataReplicatorMetrics extends DataReplicatorMetrics<TaskRelocationSnapshot, TaskRelocationEvent> {

        private RelocationDataReplicatorMetrics(String source, TitusRuntime titusRuntime) {
            super(source, false, titusRuntime);
        }

        @Override
        public void event(ReplicatorEvent<TaskRelocationSnapshot, TaskRelocationEvent> event) {
            super.event(event);
            setCacheCollectionSize("plans", event.getSnapshot().getPlans().size());
        }
    }
}
