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

package com.netflix.titus.master.integration.v3.scenario;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.agent.model.InstanceLifecycleState;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.grpc.protogen.AgentChangeEvent;
import com.netflix.titus.grpc.protogen.AgentInstance;
import com.netflix.titus.grpc.protogen.AgentInstanceGroup;
import com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.InstanceGroupLifecycleStateUpdate;
import com.netflix.titus.grpc.protogen.TierUpdate;
import com.netflix.titus.testkit.embedded.EmbeddedTitusOperations;
import com.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgentCluster;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;

import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcAgentModelConverters.toGrpcDeploymentState;
import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcAgentModelConverters.toGrpcLifecycleState;
import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcAgentModelConverters.toGrpcTier;

public class InstanceGroupScenarioBuilder {

    static final long TIMEOUT_MS = 10_000L;

    private static final Logger logger = LoggerFactory.getLogger(InstanceGroupScenarioBuilder.class);

    private final TitusStackResource titusStackResource;
    private final SimulatedTitusAgentCluster simulatedCluster;
    private final InstanceGroupsScenarioBuilder parent;

    private final AgentManagementServiceGrpc.AgentManagementServiceBlockingStub client;

    private final Observable<AgentChangeEvent> events;
    private final Subscription eventSubscription;

    private volatile AgentInstanceGroup instanceGroup;
    private final ConcurrentMap<String, AgentInstance> instances = new ConcurrentHashMap<>();

    InstanceGroupScenarioBuilder(TitusStackResource titusStackResource, EmbeddedTitusOperations titusOperations,
                                 SimulatedTitusAgentCluster simulatedAgentCluster,
                                 InstanceGroupsScenarioBuilder parent,
                                 Observable<AgentChangeEvent> events) {
        this.titusStackResource = titusStackResource;
        this.simulatedCluster = simulatedAgentCluster;
        this.client = titusOperations.getV3BlockingGrpcAgentClient();
        this.parent = parent;
        this.events = events;
        this.eventSubscription = events.subscribe(
                this::updateInstanceGroup,
                e -> logger.error("Error in event stream of {}", simulatedAgentCluster.getName(), e),
                () -> logger.info("Event stream of {} completed", simulatedAgentCluster.getName())
        );
    }

    public void shutdown() {
        eventSubscription.unsubscribe();
    }

    public boolean hasInstance(String instanceId) {
        return instances.containsKey(instanceId);
    }

    public InstanceGroupScenarioBuilder any(Consumer<InstanceScenarioBuilder> transformer) {
        checkIsKnown();
        ArrayList<AgentInstance> instancesList = new ArrayList<>(instances.values());
        Preconditions.checkElementIndex(0, instancesList.size(), "At least one agent instance available");
        Collections.shuffle(instancesList);
        transformer.accept(new InstanceScenarioBuilder(titusStackResource, instancesList.get(0)));
        return this;
    }

    public InstanceGroupScenarioBuilder tier(Tier tier) {
        checkIsKnown();

        Stopwatch timer = Stopwatch.createStarted();
        logger.info("Changing tier of {} to: {}", instanceGroup.getId(), tier);
        client.updateInstanceGroupTier(TierUpdate.newBuilder()
                .setInstanceGroupId(simulatedCluster.getName())
                .setTier(toGrpcTier(tier))
                .build()
        );
        logger.info("{} tier changed in {}ms", instanceGroup.getId(), timer.elapsed(TimeUnit.MILLISECONDS));

        return this;
    }

    public InstanceGroupScenarioBuilder lifecycleState(InstanceGroupLifecycleState lifecycleState) {
        checkIsKnown();

        Stopwatch timer = Stopwatch.createStarted();
        logger.info("Changing lifecycle state of {} to: {}", instanceGroup.getId(), lifecycleState);
        client.updateInstanceGroupLifecycleState(InstanceGroupLifecycleStateUpdate.newBuilder()
                .setInstanceGroupId(simulatedCluster.getName())
                .setLifecycleState(toGrpcLifecycleState(lifecycleState))
                .setDetail("Integration test update")
                .build()
        );
        logger.info("{} lifecycle state changed in {}ms", instanceGroup.getId(), timer.elapsed(TimeUnit.MILLISECONDS));

        return this;
    }

    public InstanceGroupScenarioBuilder expectInstancesInState(InstanceLifecycleState state) {
        checkIsKnown();
        Throwable error = checkTrigger()
                .takeUntil(tick -> hasInstancesInState(state))
                .toCompletable()
                .timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .get();
        if (error != null) {
            throw new IllegalStateException(error);
        }
        return this;
    }

    public InstanceGroupScenarioBuilder awaitDesiredSize(int expectedDesired) {
        checkIsKnown();

        Throwable error = checkTrigger()
                .takeUntil(tick -> instanceGroup.getDesired() == expectedDesired)
                .toCompletable()
                .timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .get();
        if (error != null) {
            throw new IllegalStateException(error);
        }
        return this;
    }

    private boolean hasInstancesInState(InstanceLifecycleState state) {
        if (instanceGroup == null) {
            return false;
        }
        if (instanceGroup.getDesired() != instances.size()) {
            return false;
        }
        com.netflix.titus.grpc.protogen.InstanceLifecycleState grpcState = toGrpcDeploymentState(state);
        return instances.values().stream().allMatch(i -> i.getLifecycleStatus().getState() == grpcState);
    }

    boolean isSynchronizedWithCloud() {
        if (instanceGroup == null) {
            return false;
        }
        if (instanceGroup.getDesired() != simulatedCluster.getAgents().size()) {
            return false;
        }
        return true;
    }

    private void checkIsKnown() {
        Preconditions.checkState(instanceGroup != null, "Instance group %s not discovered yet", simulatedCluster.getName());
    }

    private Observable<Boolean> checkTrigger() {
        return Observable.just(true).concatWith(events.map(e -> true));
    }

    private void updateInstanceGroup(AgentChangeEvent event) {
        switch (event.getEventCase()) {
            case INSTANCEGROUPUPDATE:
                AgentChangeEvent.InstanceGroupUpdate instanceGroupUpdate = event.getInstanceGroupUpdate();
                instanceGroup = instanceGroupUpdate.getInstanceGroup();
                break;
            case INSTANCEGROUPREMOVED:
                // TODO
                break;
            case AGENTINSTANCEUPDATE:
                AgentInstance instance = event.getAgentInstanceUpdate().getInstance();
                instances.put(instance.getId(), instance);
                break;
            case AGENTINSTANCEREMOVED:
                String toRemove = event.getAgentInstanceRemoved().getInstanceId();
                instances.remove(toRemove);
                break;
            case SNAPSHOTEND:
                // Ignore
                break;
        }
    }
}
