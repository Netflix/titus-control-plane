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

package com.netflix.titus.master.agent.service.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.service.AgentManagementException;
import com.netflix.titus.api.agent.store.AgentStore;
import com.netflix.titus.api.connector.cloud.Instance;
import com.netflix.titus.api.connector.cloud.InstanceCloudConnector;
import com.netflix.titus.api.connector.cloud.InstanceGroup;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.master.agent.service.AgentManagementConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;
import rx.Scheduler;
import rx.Single;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Func0;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;

import static com.netflix.titus.master.agent.store.AgentStoreReaper.isTaggedToRemove;
import static com.netflix.titus.master.agent.store.AgentStoreReaper.tagToRemove;

/**
 * {@link DefaultAgentCache} merges data from storage and the cloud provider. It also creates an event stream of agent
 * topology updates. It performs the following actions:
 * <ul>
 * <li>merges cloud provider {@link InstanceGroup} with {@link AgentInstanceGroup}</li>
 * <li>merges cloud provider {@link Instance} with {@link AgentInstance}</li>
 * <li>for a newly discovered {@link InstanceGroup} creates a default {@link AgentInstanceGroup} instance</li>
 * <li>for a newly discovered {@link Instance} creates a default {@link AgentInstance} instance</li>
 * <li>if a {@link InstanceGroup} or {@link Instance} is destroyed, removes it from a store after a configurable time passes</li>
 * <li>handles client updates in a thread safe way (runs all update tasks on an internal event loop)</li>
 * </ul>
 */
@Singleton
public class DefaultAgentCache implements AgentCache {

    private static final Logger logger = LoggerFactory.getLogger(DefaultAgentCache.class);

    private final Scheduler scheduler;
    private final AgentManagementConfiguration configuration;
    private final AgentStore agentStore;
    private final InstanceCloudConnector connector;
    private final Registry registry;
    private final Scheduler.Worker worker;
    private final AgentCacheMetrics metrics;

    private InstanceCache instanceCache;
    private Subscription instanceCacheSubscription;

    private volatile AgentDataSnapshot dataSnapshot = new AgentDataSnapshot();

    private final PublishSubject<CacheUpdateEvent> eventSubject = PublishSubject.create();

    @Inject
    public DefaultAgentCache(AgentManagementConfiguration configuration,
                             AgentStore agentStore,
                             InstanceCloudConnector connector,
                             Registry registry) {
        this(configuration, agentStore, connector, registry, Schedulers.computation());
    }

    DefaultAgentCache(AgentManagementConfiguration configuration,
                      AgentStore agentStore,
                      InstanceCloudConnector connector,
                      Registry registry,
                      Scheduler scheduler) {
        this.configuration = configuration;
        this.agentStore = agentStore;
        this.connector = connector;
        this.registry = registry;
        this.scheduler = scheduler;
        this.worker = scheduler.createWorker();
        this.metrics = new AgentCacheMetrics(registry);
    }

    /**
     * The initialization process consists of the following steps:
     * <ul>
     * <li>Load all previously discovered instance groups and agent instances from the store</li>
     * <li>Create {@link InstanceCache} with the known instance groups, and synchronously try to refresh this data.</li>
     * <li>If {@link InstanceCache} is not initialized within configured time, discard it and create an empty one.</li>
     * <li>If {@link InstanceCache} initialization succeeds, merge the updates with the data loaded from the store</li>
     * <p>
     * </ul>
     */
    @Activator
    public void enterActiveMode() {
        List<AgentInstanceGroup> persistedInstanceGroups = agentStore.retrieveAgentInstanceGroups()
                .filter(g -> !isTaggedToRemove(g))
                .toList()
                .toBlocking()
                .first();
        List<AgentInstance> persistedInstances = agentStore.retrieveAgentInstances().toList().toBlocking().first();

        Set<String> knownInstanceGroupIds = persistedInstanceGroups.stream().map(AgentInstanceGroup::getId).collect(Collectors.toSet());
        this.instanceCache = InstanceCache.newInstance(configuration, connector, knownInstanceGroupIds, registry, scheduler);
        setDataSnapshot(AgentDataSnapshot.initWithStaleDataSnapshot(persistedInstanceGroups, persistedInstances));

        logger.info("Started AgentCache with: {}", dataSnapshot.getInstanceGroups());

        this.instanceCacheSubscription = instanceCache.events()
                .compose(ObservableExt.head(() ->
                        Collections.singletonList(new CacheUpdateEvent(CacheUpdateType.Refreshed, CacheUpdateEvent.EMPTY_ID)))
                )
                .subscribe(
                        event -> {
                            switch (event.getType()) {
                                case Refreshed:
                                    onEventLoop(this::updateOnFullRefreshInstanceCacheEvent);
                                    break;
                                case InstanceGroup:
                                    onEventLoop(() -> updateOnInstanceGroupInstanceCacheEvent(event.getResourceId()));
                                    break;
                                case Instance:
                                    // Ignore, as instance group, and its instances are refreshed at the same time
                                    break;
                            }
                        },
                        e -> logger.error("InstanceCache events stream completed with an error", e),
                        () -> logger.info("InstanceCache events stream completed")
                );
    }

    public void shutdown() {
        if (instanceCache != null) {
            instanceCache.shutdown();
        }
        ObservableExt.safeUnsubscribe(instanceCacheSubscription);
    }

    @Override
    public List<AgentInstanceGroup> getInstanceGroups() {
        return new ArrayList<>(dataSnapshot.getInstanceGroups());
    }

    @Override
    public AgentInstanceGroup getInstanceGroup(String instanceGroupId) {
        return AgentManagementException.checkInstanceGroupFound(dataSnapshot.getInstanceGroup(instanceGroupId), instanceGroupId);
    }

    @Override
    public Optional<AgentInstanceGroup> findInstanceGroup(String instanceGroupId) {
        return Optional.ofNullable(dataSnapshot.getInstanceGroup(instanceGroupId));
    }

    @Override
    public Set<AgentInstance> getAgentInstances(String instanceGroupId) {
        return AgentManagementException.checkInstanceGroupFound(dataSnapshot.getInstances(instanceGroupId), instanceGroupId);
    }

    @Override
    public AgentInstance getAgentInstance(String instanceId) {
        return AgentManagementException.checkAgentFound(dataSnapshot.getInstance(instanceId), instanceId);
    }

    @Override
    public Optional<AgentInstance> findAgentInstance(String instanceId) {
        return Optional.ofNullable(dataSnapshot.getInstance(instanceId));
    }

    @Override
    public Completable updateInstanceGroupStore(AgentInstanceGroup instanceGroup) {
        return onEventLoopWithSubscription(() -> {
            getInstanceGroup(instanceGroup.getId());
            Set<AgentInstance> agentInstances = dataSnapshot.getInstances(instanceGroup.getId());
            if (agentInstances == null) {
                agentInstances = Collections.emptySet();
            }
            setDataSnapshot(dataSnapshot.updateInstanceGroup(instanceGroup, agentInstances));
            eventSubject.onNext(new CacheUpdateEvent(CacheUpdateType.InstanceGroup, instanceGroup.getId()));
        }).concatWith(agentStore.storeAgentInstanceGroup(instanceGroup));
    }

    @Override
    public Completable updateInstanceGroupStoreAndSyncCloud(AgentInstanceGroup instanceGroup) {
        return updateInstanceGroupStore(instanceGroup).doOnCompleted(() -> instanceCache.refreshInstanceGroup(instanceGroup.getId()));
    }

    @Override
    public Completable updateAgentInstanceStore(AgentInstance agentInstance) {
        return onEventLoopWithSubscription(() -> {
            getInstanceGroup(agentInstance.getInstanceGroupId());
            setDataSnapshot(dataSnapshot.updateAgentInstance(agentInstance));
            eventSubject.onNext(new CacheUpdateEvent(CacheUpdateType.Instance, agentInstance.getId()));
        }).concatWith(agentStore.storeAgentInstance(agentInstance));
    }

    @Override
    public Single<AgentInstanceGroup> getAndUpdateInstanceGroupStore(String instanceGroupId, Function<AgentInstanceGroup, AgentInstanceGroup> function) {
        Single<AgentInstanceGroup> single = onEventLoopWithSubscription(() -> {
            AgentInstanceGroup instanceGroup = getInstanceGroup(instanceGroupId);
            Set<AgentInstance> agentInstances = dataSnapshot.getInstances(instanceGroup.getId());
            if (agentInstances == null) {
                agentInstances = Collections.emptySet();
            }
            AgentInstanceGroup updatedInstanceGroup = function.apply(instanceGroup);
            setDataSnapshot(dataSnapshot.updateInstanceGroup(updatedInstanceGroup, agentInstances));
            eventSubject.onNext(new CacheUpdateEvent(CacheUpdateType.InstanceGroup, instanceGroupId));
            return updatedInstanceGroup;
        });
        return single.flatMap(ig -> agentStore.storeAgentInstanceGroup(ig).toSingle(() -> ig));
    }

    @Override
    public Single<AgentInstanceGroup> getAndUpdateInstanceGroupStoreAndSyncCloud(String instanceGroupId, Function<AgentInstanceGroup, AgentInstanceGroup> function) {
        return getAndUpdateInstanceGroupStore(instanceGroupId, function)
                .flatMap(ig -> {
                    instanceCache.refreshInstanceGroup(instanceGroupId);
                    return Single.just(ig);
                });
    }

    @Override
    public Single<AgentInstance> getAndUpdateAgentInstanceStore(String instanceId, Function<AgentInstance, AgentInstance> function) {
        Single<AgentInstance> single = onEventLoopWithSubscription(() -> {
            AgentInstance agentInstance = getAgentInstance(instanceId);
            AgentInstance updatedAgentInstance = function.apply(agentInstance);
            setDataSnapshot(dataSnapshot.updateAgentInstance(updatedAgentInstance));
            eventSubject.onNext(new CacheUpdateEvent(CacheUpdateType.Instance, instanceId));
            return updatedAgentInstance;
        });
        return single.flatMap(ai -> agentStore.storeAgentInstance(ai).toSingle(() -> ai));
    }

    @Override
    public Completable removeInstances(String instanceGroupId, Set<String> agentInstanceIds) {
        return onEventLoopWithSubscription(() ->
                setDataSnapshot(dataSnapshot.removeInstances(instanceGroupId, agentInstanceIds))
        ).concatWith(agentStore.removeAgentInstances(new ArrayList<>(agentInstanceIds)));
    }

    @Override
    public void forceRefresh() {
        instanceCache.doFullInstanceGroupRefresh().subscribe(
                () -> logger.info("Forced full instance cache refresh finished"),
                e -> logger.info("Forced full instance cache refresh failed", e)
        );
    }

    @Override
    public Observable<CacheUpdateEvent> events() {
        return eventSubject;
    }

    private void updateOnFullRefreshInstanceCacheEvent() {
        Set<String> knownInstanceGroupIds = dataSnapshot.getInstanceGroupIds();
        List<InstanceGroup> allInstanceGroups = instanceCache.getInstanceGroups();
        Set<String> allInstanceGroupIds = allInstanceGroups.stream().map(InstanceGroup::getId).collect(Collectors.toSet());

        Set<String> newInstanceGroupIds = allInstanceGroups.stream()
                .map(InstanceGroup::getId)
                .filter(id -> !knownInstanceGroupIds.contains(id))
                .collect(Collectors.toSet());
        Set<String> removedInstanceGroupIds = knownInstanceGroupIds.stream().filter(id -> !allInstanceGroupIds.contains(id)).collect(Collectors.toSet());
        List<AgentInstanceGroup> removedInstanceGroups = removedInstanceGroupIds.stream().map(id -> dataSnapshot.getInstanceGroup(id)).collect(Collectors.toList());

        newInstanceGroupIds.forEach(this::syncInstanceGroupWithInstanceCache);
        removedInstanceGroupIds.forEach(this::syncInstanceGroupWithInstanceCache);

        newInstanceGroupIds.forEach(this::storeEagerly);
        removedInstanceGroups.forEach(this::storeEagerlyWithRemoveFlag);
    }

    private void updateOnInstanceGroupInstanceCacheEvent(String instanceGroupId) {
        AgentInstanceGroup instanceGroup = getInstanceGroup(instanceGroupId);
        if (instanceGroup != null) {
            syncInstanceGroupWithInstanceCache(instanceGroupId);
            if (instanceCache.getInstanceGroup(instanceGroupId) == null) {
                storeEagerlyWithRemoveFlag(instanceGroup);
            }
        }
    }

    private void syncInstanceGroupWithInstanceCache(String instanceGroupId) {
        InstanceGroup instanceGroup = instanceCache.getInstanceGroup(instanceGroupId);

        if (instanceGroup == null) {
            setDataSnapshot(dataSnapshot.removeInstanceGroup(instanceGroupId));
            eventSubject.onNext(new CacheUpdateEvent(CacheUpdateType.InstanceGroup, instanceGroupId));
            logger.debug("instance group: {} no longer exists", instanceGroupId);
            return;
        }

        AgentInstanceGroup previous = dataSnapshot.getInstanceGroup(instanceGroupId);
        AgentInstanceGroup agentInstanceGroup;
        List<AgentInstance> agentInstances;
        if (previous == null) {
            String instanceType = instanceGroup.getInstanceType();
            ResourceDimension instanceResourceDimension;
            try {
                instanceResourceDimension = connector.getInstanceTypeResourceDimension(instanceType);
            } catch (Exception e) {
                logger.warn("Cannot resolve resource dimension for instance type: {}", instanceType);
                instanceResourceDimension = ResourceDimension.empty();
            }

            agentInstanceGroup = DataConverters.toAgentInstanceGroup(
                    instanceGroup,
                    instanceResourceDimension
            );
            agentInstances = instanceGroup.getInstanceIds().stream()
                    .map(instanceCache::getAgentInstance)
                    .filter(Objects::nonNull)
                    .map(DataConverters::toAgentInstance)
                    .collect(Collectors.toList());
        } else {
            agentInstanceGroup = DataConverters.updateAgentInstanceGroup(previous, instanceGroup);
            agentInstances = instanceGroup.getInstanceIds().stream()
                    .map(id -> {
                        Instance instance = instanceCache.getAgentInstance(id);
                        if (instance == null) {
                            return null;
                        }
                        AgentInstance previousInstance = dataSnapshot.getInstance(id);
                        if (previousInstance == null) {
                            return DataConverters.toAgentInstance(instance);
                        }
                        return DataConverters.updateAgentInstance(previousInstance, instance);
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        }
        TreeSet<AgentInstance> agentInstanceSet = new TreeSet<>(AgentInstance.idComparator());
        agentInstanceSet.addAll(agentInstances);
        logger.debug("Creating new agent data snapshot for instance group: {} with instances: {}", instanceGroupId, agentInstanceSet);

        setDataSnapshot(dataSnapshot.updateInstanceGroup(agentInstanceGroup, agentInstanceSet));
        eventSubject.onNext(new CacheUpdateEvent(CacheUpdateType.InstanceGroup, instanceGroupId));
    }

    private void storeEagerly(String instanceGroupId) {
        AgentInstanceGroup instanceGroup = dataSnapshot.getInstanceGroup(instanceGroupId);
        if (instanceGroup != null) {
            agentStore.storeAgentInstanceGroup(instanceGroup).subscribe(
                    () -> logger.info("Persisted instance group: {} to the store", instanceGroup.getId()),
                    e -> logger.warn("Could not persist instance group: {} the store", instanceGroup.getId(), e)
            );
        }
    }

    private void storeEagerlyWithRemoveFlag(AgentInstanceGroup instanceGroup) {
        if (!isTaggedToRemove(instanceGroup)) {
            agentStore.storeAgentInstanceGroup(tagToRemove(instanceGroup, scheduler)).subscribe(
                    () -> logger.info("Tagging instance group: {} as removed", instanceGroup.getId()),
                    e -> logger.warn("Could not persist instance group: {} into the store", instanceGroup.getId(), e)
            );
        }
    }

    private void onEventLoop(Action0 action) {
        worker.schedule(() -> {
            try {
                action.call();
            } catch (Exception e) {
                logger.warn("AgentCache internal operation error", e);
            }
        });
    }

    private Completable onEventLoopWithSubscription(Action0 action) {
        return Observable.unsafeCreate(subscriber -> {
            Subscription subscription = worker.schedule(() -> {
                try {
                    action.call();
                    subscriber.onCompleted();
                } catch (Throwable e) {
                    subscriber.onError(e);
                }
            });
            subscriber.add(subscription);
        }).toCompletable();
    }

    private <T> Single<T> onEventLoopWithSubscription(Func0<T> function) {
        return Single.create(subscriber -> {
            Subscription subscription = worker.schedule(() -> {
                try {
                    subscriber.onSuccess(function.call());
                } catch (Throwable e) {
                    subscriber.onError(e);
                }
            });
            subscriber.add(subscription);
        });
    }

    private void setDataSnapshot(AgentDataSnapshot dataSnapshot) {
        this.dataSnapshot = dataSnapshot;
        metrics.refresh(dataSnapshot);
    }
}
