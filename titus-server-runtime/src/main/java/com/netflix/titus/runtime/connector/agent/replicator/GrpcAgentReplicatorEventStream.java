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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.event.AgentEvent;
import com.netflix.titus.api.agent.model.event.AgentInstanceGroupRemovedEvent;
import com.netflix.titus.api.agent.model.event.AgentInstanceGroupUpdateEvent;
import com.netflix.titus.api.agent.model.event.AgentInstanceRemovedEvent;
import com.netflix.titus.api.agent.model.event.AgentInstanceUpdateEvent;
import com.netflix.titus.api.agent.model.event.AgentSnapshotEndEvent;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.runtime.connector.agent.AgentManagementClient;
import com.netflix.titus.runtime.connector.agent.AgentSnapshot;
import com.netflix.titus.runtime.connector.common.replicator.AbstractReplicatorEventStream;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicatorMetrics;
import com.netflix.titus.runtime.connector.common.replicator.ReplicatorEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

public class GrpcAgentReplicatorEventStream extends AbstractReplicatorEventStream<AgentSnapshot, AgentEvent> {

    private static final Logger logger = LoggerFactory.getLogger(GrpcAgentReplicatorEventStream.class);

    private final AgentManagementClient client;

    public GrpcAgentReplicatorEventStream(AgentManagementClient client,
                                          DataReplicatorMetrics metrics,
                                          TitusRuntime titusRuntime,
                                          Scheduler scheduler) {
        super(metrics, titusRuntime, scheduler);
        this.client = client;
    }

    @Override
    protected Flux<ReplicatorEvent<AgentSnapshot, AgentEvent>> newConnection() {
        return Flux.defer(() -> {
            CacheUpdater cacheUpdater = new CacheUpdater();
            logger.info("Connecting to the agent event stream...");
            return client.observeAgents().flatMap(cacheUpdater::onEvent);
        });
    }

    private class CacheUpdater {

        private final Map<String, AgentEvent> snapshotEvents = new HashMap<>();
        private final AtomicReference<AgentSnapshot> lastAgentSnapshotRef = new AtomicReference<>();

        private Flux<ReplicatorEvent<AgentSnapshot, AgentEvent>> onEvent(AgentEvent event) {
            try {
                if (lastAgentSnapshotRef.get() != null) {
                    return processSnapshotUpdate(event);
                }
                if (event instanceof AgentSnapshotEndEvent) {
                    return buildInitialCache();
                }

                if (event instanceof AgentInstanceGroupUpdateEvent) {
                    snapshotEvents.put(((AgentInstanceGroupUpdateEvent) event).getAgentInstanceGroup().getId(), event);
                } else if (event instanceof AgentInstanceGroupRemovedEvent) {
                    snapshotEvents.remove(((AgentInstanceGroupRemovedEvent) event).getInstanceGroupId());
                } else if (event instanceof AgentInstanceUpdateEvent) {
                    snapshotEvents.put(((AgentInstanceUpdateEvent) event).getAgentInstance().getId(), event);
                } else if (event instanceof AgentInstanceRemovedEvent) {
                    snapshotEvents.remove(((AgentInstanceRemovedEvent) event).getAgentInstanceId(), event);
                }
            } catch (Exception e) {
                logger.warn("Unexpected error when handling the agent change notification: {}", event, e);
                return Flux.error(e); // Return error to force the cache reconnect.
            }
            return Flux.empty();
        }

        private Flux<ReplicatorEvent<AgentSnapshot, AgentEvent>> buildInitialCache() {
            Map<String, com.netflix.titus.api.agent.model.AgentInstanceGroup> instanceGroupsById = new HashMap<>();
            Map<String, List<com.netflix.titus.api.agent.model.AgentInstance>> instancesByGroupId = new HashMap<>();

            snapshotEvents.forEach((id, event) -> {
                if (event instanceof AgentInstanceGroupUpdateEvent) {
                    instanceGroupsById.put(id, ((AgentInstanceGroupUpdateEvent) event).getAgentInstanceGroup());
                } else if (event instanceof AgentInstanceUpdateEvent) {
                    AgentInstance instance = ((AgentInstanceUpdateEvent) event).getAgentInstance();
                    instancesByGroupId.computeIfAbsent(instance.getInstanceGroupId(), gid -> new ArrayList<>()).add(instance);
                }
            });

            // Clear so the garbage collector can reclaim the memory (we no longer need this data).
            snapshotEvents.clear();

            AgentSnapshot initialSnapshot = new AgentSnapshot(UUID.randomUUID().toString(), instanceGroupsById, instancesByGroupId);
            lastAgentSnapshotRef.set(initialSnapshot);

            logger.info("Agent snapshot loaded: {}", initialSnapshot.toSummaryString());

            return Flux.just(new ReplicatorEvent<>(initialSnapshot, AgentSnapshotEndEvent.snapshotEnd(), titusRuntime.getClock().wallTime()));
        }

        private Flux<ReplicatorEvent<AgentSnapshot, AgentEvent>> processSnapshotUpdate(AgentEvent event) {
            logger.debug("Processing agent snapshot update event: {}", event);

            AgentSnapshot lastSnapshot = lastAgentSnapshotRef.get();
            Optional<AgentSnapshot> newSnapshot;
            if (event instanceof AgentInstanceGroupUpdateEvent) {
                newSnapshot = lastSnapshot.updateInstanceGroup(((AgentInstanceGroupUpdateEvent) event).getAgentInstanceGroup());
            } else if (event instanceof AgentInstanceGroupRemovedEvent) {
                newSnapshot = lastSnapshot.removeInstanceGroup(((AgentInstanceGroupRemovedEvent) event).getInstanceGroupId());
            } else if (event instanceof AgentInstanceUpdateEvent) {
                newSnapshot = lastSnapshot.updateInstance(((AgentInstanceUpdateEvent) event).getAgentInstance());
            } else if (event instanceof AgentInstanceRemovedEvent) {
                newSnapshot = lastSnapshot.removeInstance(((AgentInstanceRemovedEvent) event).getAgentInstanceId());
            } else {
                newSnapshot = Optional.empty();
            }
            if (newSnapshot.isPresent()) {
                lastAgentSnapshotRef.set(newSnapshot.get());
                return Flux.just(new ReplicatorEvent<>(newSnapshot.get(), event, titusRuntime.getClock().wallTime()));
            }
            return Flux.empty();
        }
    }
}
