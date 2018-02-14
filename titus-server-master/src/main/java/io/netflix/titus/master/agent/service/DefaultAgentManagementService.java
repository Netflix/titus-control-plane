/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.agent.service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import io.netflix.titus.api.agent.model.AgentInstance;
import io.netflix.titus.api.agent.model.AgentInstanceGroup;
import io.netflix.titus.api.agent.model.AutoScaleRule;
import io.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import io.netflix.titus.api.agent.model.InstanceGroupLifecycleStatus;
import io.netflix.titus.api.agent.model.InstanceOverrideStatus;
import io.netflix.titus.api.agent.model.event.AgentEvent;
import io.netflix.titus.api.agent.service.AgentManagementException;
import io.netflix.titus.api.agent.service.AgentManagementService;
import io.netflix.titus.api.connector.cloud.InstanceCloudConnector;
import io.netflix.titus.api.model.Tier;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.common.util.guice.annotation.Activator;
import io.netflix.titus.common.util.guice.annotation.ProxyConfiguration;
import io.netflix.titus.common.util.rx.ObservableExt;
import io.netflix.titus.common.util.tuple.Either;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.agent.service.cache.AgentCache;
import rx.Completable;
import rx.Observable;

import static io.netflix.titus.common.util.guice.ProxyType.ActiveGuard;
import static io.netflix.titus.common.util.guice.ProxyType.Logging;
import static io.netflix.titus.common.util.guice.ProxyType.Spectator;

@Singleton
@ProxyConfiguration(types = {Logging, Spectator, ActiveGuard})
public class DefaultAgentManagementService implements AgentManagementService {

    private final AgentManagementConfiguration configuration;
    private final InstanceCloudConnector instanceCloudConnector;
    private final AgentCache agentCache;

    @Inject
    public DefaultAgentManagementService(AgentManagementConfiguration configuration,
                                         InstanceCloudConnector instanceCloudConnector,
                                         AgentCache AgentCache) {
        this.configuration = configuration;
        this.instanceCloudConnector = instanceCloudConnector;
        this.agentCache = AgentCache;
    }

    @Activator
    public void enterActiveMode() {
        // We need this empty method, to mark this service as activated.
    }

    @Override
    public List<AgentInstanceGroup> getInstanceGroups() {
        return agentCache.getInstanceGroups();
    }

    @Override
    public AgentInstanceGroup getInstanceGroup(String id) {
        return agentCache.getInstanceGroup(id);
    }

    @Override
    public AgentInstance getAgentInstance(String id) {
        return agentCache.getAgentInstance(id);
    }

    @Override
    public List<AgentInstance> getAgentInstances(String instanceGroupId) {
        return new ArrayList<>(agentCache.getAgentInstances(instanceGroupId));
    }

    @Override
    public List<Pair<AgentInstanceGroup, List<AgentInstance>>> findAgentInstances(Predicate<Pair<AgentInstanceGroup, AgentInstance>> filter) {
        return agentCache.getInstanceGroups().stream()
                .map(instanceGroup -> {
                    List<AgentInstance> matchingAgents = agentCache.getAgentInstances(instanceGroup.getId()).stream()
                            .filter(agent -> filter.test(Pair.of(instanceGroup, agent)))
                            .collect(Collectors.toList());
                    return Pair.of(instanceGroup, matchingAgents);
                })
                .filter(pair -> !pair.getRight().isEmpty())
                .collect(Collectors.toList());
    }

    @Override
    public Completable updateInstanceGroupTier(String instanceGroupId, Tier tier) {
        return updateServerGroupInStore(
                instanceGroupId,
                instanceGroup -> instanceGroup.toBuilder().withTier(tier).build()
        );
    }

    @Override
    public Completable updateAutoScalingRule(String instanceGroupId, AutoScaleRule autoScaleRule) {
        return updateServerGroupInStore(
                instanceGroupId,
                instanceGroup -> instanceGroup.toBuilder().withAutoScaleRule(autoScaleRule).build()
        );
    }

    @Override
    public Completable updateInstanceGroupLifecycle(String instanceGroupId, InstanceGroupLifecycleStatus instanceGroupLifecycleStatus) {
        return Observable.fromCallable(() -> getInstanceGroup(instanceGroupId))
                .flatMap(instanceGroup -> {
                    AgentInstanceGroup newInstanceGroup = instanceGroup.toBuilder().withLifecycleStatus(instanceGroupLifecycleStatus).build();
                    Completable updateStoreCompletable = agentCache.updateInstanceGroupStore(newInstanceGroup);

                    if (instanceGroupLifecycleStatus.getState() == InstanceGroupLifecycleState.Removable) {
                        // Force the min to 0 when we change the state to Removable
                        return updateStoreCompletable.andThen(internalUpdateCapacity(newInstanceGroup, Optional.of(0), Optional.empty())).toObservable();
                    } else {
                        return updateStoreCompletable.toObservable();
                    }
                }).toCompletable();
    }

    @Override
    public Completable updateCapacity(String agentServerGroupId, Optional<Integer> min, Optional<Integer> desired) {
        if (!configuration.isInstanceGroupUpdateCapacityEnabled()) {
            return Completable.complete();
        }
        return Observable.fromCallable(() -> agentCache.getInstanceGroup(agentServerGroupId))
                .flatMap(instanceGroup -> {
                            if (instanceGroup.getLifecycleStatus().getState() == InstanceGroupLifecycleState.Removable && min.isPresent()) {
                                return Observable.error(AgentManagementException.invalidArgument("Min cannot be set in the Removable state"));
                            }
                            return internalUpdateCapacity(instanceGroup, min, desired).toObservable();
                        }
                )
                .toCompletable();
    }

    @Override
    public Completable scaleUp(String instanceGroupId, int scaleUpCount) {
        if (!configuration.isInstanceGroupUpdateCapacityEnabled()) {
            return Completable.complete();
        }
        return Observable.fromCallable(() -> agentCache.getInstanceGroup(instanceGroupId))
                .flatMap(instanceGroup -> {
                            Completable cloudUpdate = instanceCloudConnector.scaleUp(instanceGroup.getId(), scaleUpCount);

                            AgentInstanceGroup updated = instanceGroup.toBuilder().withDesired(instanceGroup.getDesired() + scaleUpCount).build();
                            Completable cacheUpdate = agentCache.updateInstanceGroupStoreAndSyncCloud(updated);

                            return cloudUpdate.concatWith(cacheUpdate).toObservable();
                        }
                )
                .toCompletable();
    }

    @Override
    public Completable updateInstanceOverride(String agentServerId, InstanceOverrideStatus instanceOverrideStatus) {
        return Observable.fromCallable(() -> agentCache.getAgentInstance(agentServerId))
                .flatMap(agentInstance -> {
                            AgentInstance updated = agentInstance.toBuilder().withOverrideStatus(instanceOverrideStatus).build();
                            return agentCache.updateAgentInstanceStore(updated).toObservable();
                        }
                ).toCompletable();
    }

    @Override
    public Completable removeInstanceOverride(String agentServerId) {
        return updateInstanceOverride(agentServerId, InstanceOverrideStatus.none());
    }

    @Override
    public Observable<List<Either<Boolean, Throwable>>> terminateAgents(String agentServerGroupId, List<String> agentInstanceIds, boolean shrink) {
        if (!configuration.isInstanceGroupUpdateCapacityEnabled() || agentInstanceIds.isEmpty()) {
            return Observable.empty();
        }
        return Observable.fromCallable(() -> resolveServerGroup(agentInstanceIds))
                .flatMap(instanceGroupId ->
                        instanceCloudConnector.terminateInstances(agentServerGroupId, agentInstanceIds, shrink)
                                .flatMap(resultList -> {
                                    Set<String> removedInstanceIds = new HashSet<>();
                                    for (int i = 0; i < resultList.size(); i++) {
                                        Either<Boolean, Throwable> result = resultList.get(i);
                                        if (result.hasValue() && result.getValue()) {
                                            removedInstanceIds.add(agentInstanceIds.get(i));
                                        }
                                    }
                                    if (removedInstanceIds.isEmpty()) {
                                        return Observable.empty();
                                    }
                                    Observable cacheUpdate = agentCache.removeInstances(agentServerGroupId, removedInstanceIds).toObservable();
                                    return (Observable<List<Either<Boolean, Throwable>>>) cacheUpdate.concatWith(Observable.just(resultList));
                                })
                );
    }

    @Override
    public void forceRefresh() {
        agentCache.forceRefresh();
    }

    @Override
    public Observable<AgentEvent> events(boolean includeSnapshot) {
        return Observable.fromCallable(() -> new AgentEventEmitter(agentCache)).flatMap(initial -> {
            Observable<AgentEvent> eventObservable = agentCache.events()
                    .compose(ObservableExt.mapWithState(initial, (event, state) -> state.apply(event)))
                    .flatMap(Observable::from);

            // Compensate for notifications that we might lost during stream initialization. We may emit duplicates
            // at this stage, but this is ok.
            eventObservable = eventObservable.compose(ObservableExt.head(initial::compareWithLatestData));

            if (includeSnapshot) {
                eventObservable = Observable.from(initial.emitSnapshot()).concatWith(eventObservable);
            }

            return eventObservable;
        });
    }

    private Completable updateServerGroupInStore(String instanceGroupId, Function<AgentInstanceGroup, AgentInstanceGroup> updater) {
        return Observable.fromCallable(() -> getInstanceGroup(instanceGroupId))
                .flatMap(instanceGroup -> {
                    AgentInstanceGroup newServerGroup = updater.apply(instanceGroup);
                    return agentCache.updateInstanceGroupStore(newServerGroup).toObservable();
                }).toCompletable();
    }

    private String resolveServerGroup(List<String> instanceIds) {
        Set<String> instanceGroupIds = instanceIds.stream()
                .map(instanceId -> agentCache.getAgentInstance(instanceId).getInstanceGroupId())
                .collect(Collectors.toSet());
        AgentManagementException.checkArgument(
                instanceGroupIds.size() == 1,
                "Instances %s belong to different instance groups: %s", instanceIds, instanceGroupIds
        );
        return CollectionsExt.first(instanceGroupIds);
    }

    private Completable internalUpdateCapacity(AgentInstanceGroup instanceGroup, Optional<Integer> min, Optional<Integer> desired) {
        Completable cloudUpdate = instanceCloudConnector.updateCapacity(instanceGroup.getId(), min, desired);

        AgentInstanceGroup.Builder builder = instanceGroup.toBuilder();
        min.ifPresent(builder::withMin);
        desired.ifPresent(builder::withDesired);
        Completable cacheUpdate = agentCache.updateInstanceGroupStoreAndSyncCloud(builder.build());

        return cloudUpdate.concatWith(cacheUpdate);
    }
}
