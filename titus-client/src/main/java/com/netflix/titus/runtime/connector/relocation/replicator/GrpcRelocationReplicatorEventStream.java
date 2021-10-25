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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.titus.api.relocation.model.event.TaskRelocationEvent;
import com.netflix.titus.api.relocation.model.event.TaskRelocationPlanRemovedEvent;
import com.netflix.titus.api.relocation.model.event.TaskRelocationPlanUpdateEvent;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.grpc.protogen.TaskRelocationQuery;
import com.netflix.titus.runtime.connector.common.replicator.AbstractReplicatorEventStream;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicatorMetrics;
import com.netflix.titus.runtime.connector.common.replicator.ReplicatorEvent;
import com.netflix.titus.runtime.connector.relocation.RelocationServiceClient;
import com.netflix.titus.runtime.connector.relocation.TaskRelocationSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

public class GrpcRelocationReplicatorEventStream extends AbstractReplicatorEventStream<TaskRelocationSnapshot, TaskRelocationEvent> {

    private static final Logger logger = LoggerFactory.getLogger(GrpcRelocationReplicatorEventStream.class);

    private final RelocationServiceClient client;

    public GrpcRelocationReplicatorEventStream(RelocationServiceClient client,
                                               DataReplicatorMetrics metrics,
                                               TitusRuntime titusRuntime,
                                               Scheduler scheduler) {
        super(false, TaskRelocationEvent.newKeepAliveEvent(), metrics, titusRuntime, scheduler);
        this.client = client;
    }

    @Override
    protected Flux<ReplicatorEvent<TaskRelocationSnapshot, TaskRelocationEvent>> newConnection() {
        return Flux.defer(() -> {
            CacheUpdater cacheUpdater = new CacheUpdater();
            logger.info("Connecting to the task relocation event stream...");
            return client.events(TaskRelocationQuery.getDefaultInstance()).flatMap(cacheUpdater::onEvent);
        });
    }

    private class CacheUpdater {

        private final List<TaskRelocationEvent> snapshotEvents = new ArrayList<>();
        private final AtomicReference<TaskRelocationSnapshot> lastSnapshotRef = new AtomicReference<>();

        public Flux<ReplicatorEvent<TaskRelocationSnapshot, TaskRelocationEvent>> onEvent(TaskRelocationEvent event) {
            try {
                if (lastSnapshotRef.get() != null) {
                    return processSnapshotUpdate(event);
                }
                if (event.equals(TaskRelocationEvent.newSnapshotEndEvent())) {
                    return buildInitialCache();
                }
                snapshotEvents.add(event);
            } catch (Exception e) {
                logger.warn("Unexpected error when handling the relocation event: {}", event, e);
                return Flux.error(e); // Return error to force the cache reconnect.
            }
            return Flux.empty();
        }

        private Flux<ReplicatorEvent<TaskRelocationSnapshot, TaskRelocationEvent>> buildInitialCache() {
            TaskRelocationSnapshot.Builder builder = TaskRelocationSnapshot.newBuilder();
            snapshotEvents.forEach(event -> applyToBuilder(builder, event));
            TaskRelocationSnapshot snapshot = builder.build();

            // No longer needed
            snapshotEvents.clear();

            logger.info("Relocation snapshot loaded: {}", snapshot.toSummaryString());

            lastSnapshotRef.set(snapshot);

            return Flux.just(new ReplicatorEvent<>(snapshot, TaskRelocationEvent.newSnapshotEndEvent(), titusRuntime.getClock().wallTime()));
        }

        private Flux<ReplicatorEvent<TaskRelocationSnapshot, TaskRelocationEvent>> processSnapshotUpdate(TaskRelocationEvent event) {
            logger.debug("Processing task relocation event: {}", event);

            TaskRelocationSnapshot.Builder builder = lastSnapshotRef.get().toBuilder();
            applyToBuilder(builder, event);
            TaskRelocationSnapshot newSnapshot = builder.build();

            lastSnapshotRef.set(newSnapshot);

            return Flux.just(new ReplicatorEvent<>(newSnapshot, event, titusRuntime.getClock().wallTime()));
        }

        private void applyToBuilder(TaskRelocationSnapshot.Builder builder, TaskRelocationEvent event) {
            if (event instanceof TaskRelocationPlanUpdateEvent) {
                builder.addPlan(((TaskRelocationPlanUpdateEvent) event).getPlan());
            } else if (event instanceof TaskRelocationPlanRemovedEvent) {
                builder.removePlan(((TaskRelocationPlanRemovedEvent) event).getTaskId());
            }
        }
    }
}
