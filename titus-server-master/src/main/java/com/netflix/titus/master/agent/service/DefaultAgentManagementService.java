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

package com.netflix.titus.master.agent.service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleStatus;
import com.netflix.titus.api.agent.model.event.AgentEvent;
import com.netflix.titus.api.agent.service.AgentManagementException;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.connector.cloud.InstanceCloudConnector;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.guice.annotation.ProxyConfiguration;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.common.util.tuple.Either;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.agent.ServerInfo;
import com.netflix.titus.master.agent.service.cache.AgentCache;
import com.netflix.titus.master.agent.service.server.ServerInfoResolver;
import rx.Completable;
import rx.Observable;

import static com.netflix.titus.common.util.guice.ProxyType.ActiveGuard;

@Singleton
@ProxyConfiguration(types = {ActiveGuard})
public class DefaultAgentManagementService implements AgentManagementService {

    private final AgentManagementConfiguration configuration;
    private final InstanceCloudConnector instanceCloudConnector;
    private final AgentCache agentCache;
    private final ServerInfoResolver serverInfoResolver;

    @Inject
    public DefaultAgentManagementService(AgentManagementConfiguration configuration,
                                         InstanceCloudConnector instanceCloudConnector,
                                         AgentCache AgentCache,
                                         ServerInfoResolver serverInfoResolver) {
        this.configuration = configuration;
        this.instanceCloudConnector = instanceCloudConnector;
        this.agentCache = AgentCache;
        this.serverInfoResolver = serverInfoResolver;
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
    public AgentInstanceGroup getInstanceGroup(String instanceGroupId) {
        return agentCache.getInstanceGroup(instanceGroupId);
    }

    @Override
    public Optional<AgentInstanceGroup> findInstanceGroup(String instanceGroupId) {
        return agentCache.findInstanceGroup(instanceGroupId);
    }

    @Override
    public AgentInstance getAgentInstance(String instanceId) {
        return agentCache.getAgentInstance(instanceId);
    }

    @Override
    public Optional<AgentInstance> findAgentInstance(String instanceId) {
        return agentCache.findAgentInstance(instanceId);
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
    public ResourceDimension getResourceLimits(String instanceType) {
        return findResourceLimits(instanceType).orElseThrow(() -> AgentManagementException.instanceTypeNotFound(instanceType));
    }

    @Override
    public Optional<ResourceDimension> findResourceLimits(String instanceType) {
        ResourceDimension result = serverInfoResolver.resolve(instanceType)
                .map(ServerInfo::toResourceDimension)
                .orElseGet(() -> getInstanceGroups().stream()
                        .filter(instanceGroup -> instanceType.equals(instanceGroup.getInstanceType()))
                        .findFirst()
                        .map(AgentInstanceGroup::getResourceDimension)
                        .orElse(null)
                );
        return Optional.ofNullable(result);
    }

    @Override
    public Completable updateInstanceGroupTier(String instanceGroupId, Tier tier) {
        return agentCache.getAndUpdateInstanceGroupStore(
                instanceGroupId,
                instanceGroup -> instanceGroup.toBuilder().withTier(tier).build()
        ).toCompletable();
    }

    @Override
    public Completable updateInstanceGroupLifecycle(String instanceGroupId, InstanceGroupLifecycleStatus instanceGroupLifecycleStatus) {
        return agentCache.getAndUpdateInstanceGroupStore(
                instanceGroupId,
                instanceGroup -> instanceGroup.toBuilder().withLifecycleStatus(instanceGroupLifecycleStatus).build()
        ).flatMapCompletable(instanceGroup -> {
            if (instanceGroupLifecycleStatus.getState() == InstanceGroupLifecycleState.Removable) {
                // Force the min to 0 when we change the state to Removable
                return internalUpdateCapacity(instanceGroup, Optional.of(0), Optional.empty());
            }
            return Completable.complete();
        });
    }

    @Override
    public Completable updateInstanceGroupAttributes(String instanceGroupId, Map<String, String> attributes) {
        return agentCache.getAndUpdateInstanceGroupStore(
                instanceGroupId,
                instanceGroup -> {
                    Map<String, String> updatedAttributes = CollectionsExt.merge(instanceGroup.getAttributes(), attributes);
                    return instanceGroup.toBuilder().withAttributes(updatedAttributes).build();
                }
        ).toCompletable();
    }

    @Override
    public Completable deleteInstanceGroupAttributes(String instanceGroupId, List<String> keys) {
        return agentCache.getAndUpdateInstanceGroupStore(
                instanceGroupId,
                instanceGroup -> {
                    Map<String, String> updatedAttributes = CollectionsExt.copyAndRemove(instanceGroup.getAttributes(), keys);
                    return instanceGroup.toBuilder().withAttributes(updatedAttributes).build();
                }
        ).toCompletable();
    }

    @Override
    public Completable updateAgentInstanceAttributes(String instanceId, Map<String, String> attributes) {
        return agentCache.getAndUpdateAgentInstanceStore(
                instanceId,
                agentInstance -> {
                    Map<String, String> updatedAttributes = CollectionsExt.merge(agentInstance.getAttributes(), attributes);
                    return agentInstance.toBuilder().withAttributes(updatedAttributes).build();
                }
        ).toCompletable();
    }

    @Override
    public Completable deleteAgentInstanceAttributes(String instanceId, List<String> keys) {
        return agentCache.getAndUpdateAgentInstanceStore(
                instanceId,
                agentInstance -> {
                    Map<String, String> updatedAttributes = CollectionsExt.copyAndRemove(agentInstance.getAttributes(), keys);
                    return agentInstance.toBuilder().withAttributes(updatedAttributes).build();
                }
        ).toCompletable();
    }

    @Override
    public Completable updateCapacity(String instanceGroupId, Optional<Integer> min, Optional<Integer> desired) {
        if (!configuration.isInstanceGroupUpdateCapacityEnabled()) {
            return Completable.complete();
        }
        return Observable.fromCallable(() -> agentCache.getInstanceGroup(instanceGroupId))
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
                            Completable cacheUpdate = agentCache.getAndUpdateInstanceGroupStoreAndSyncCloud(
                                    instanceGroup.getId(),
                                    ig -> ig.toBuilder().withDesired(ig.getDesired() + scaleUpCount).build()
                            ).toCompletable();
                            return cloudUpdate.concatWith(cacheUpdate).toObservable();
                        }
                )
                .toCompletable();
    }

    @Override
    public Observable<List<Either<Boolean, Throwable>>> terminateAgents(String instanceGroupId, List<String> instanceIds, boolean shrink) {
        if (!configuration.isInstanceGroupUpdateCapacityEnabled() || instanceIds.isEmpty()) {
            return Observable.empty();
        }
        return Observable.fromCallable(() -> resolveInstanceGroup(instanceIds))
                .flatMap(id -> {
                            AgentInstanceGroup instanceGroup = agentCache.getInstanceGroup(id);
                            int differenceBetweenMinAndCurrent = instanceGroup.getCurrent() - instanceGroup.getMin();
                            if (instanceIds.size() > differenceBetweenMinAndCurrent) {
                                return Observable.error(AgentManagementException.invalidArgument(
                                        "Cannot terminate %d agents as the current(%d) would be less than the min(%d)",
                                        instanceIds.size(), instanceGroup.getCurrent(), instanceGroup.getMin()
                                ));
                            }
                            return instanceCloudConnector.terminateInstances(id, instanceIds, shrink)
                                    .flatMap(resultList -> {
                                        Set<String> removedInstanceIds = new HashSet<>();
                                        for (int i = 0; i < resultList.size(); i++) {
                                            Either<Boolean, Throwable> result = resultList.get(i);
                                            if (result.hasValue() && result.getValue()) {
                                                removedInstanceIds.add(instanceIds.get(i));
                                            }
                                        }
                                        if (removedInstanceIds.isEmpty()) {
                                            return Observable.empty();
                                        }
                                        Observable cacheUpdate = agentCache.removeInstances(instanceGroupId, removedInstanceIds).toObservable();
                                        return (Observable<List<Either<Boolean, Throwable>>>) cacheUpdate.concatWith(Observable.just(resultList));
                                    });
                        }
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

    private String resolveInstanceGroup(List<String> instanceIds) {
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

        Completable cacheUpdate = agentCache.getAndUpdateInstanceGroupStoreAndSyncCloud(instanceGroup.getId(), ig -> {
            AgentInstanceGroup.Builder builder = ig.toBuilder();
            min.ifPresent(builder::withMin);
            desired.ifPresent(builder::withDesired);
            return builder.build();
        }).toCompletable();

        return cloudUpdate.concatWith(cacheUpdate);
    }
}
