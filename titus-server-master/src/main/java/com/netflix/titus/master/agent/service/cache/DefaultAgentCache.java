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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.collect.Sets;
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
import com.netflix.titus.common.util.rx.SchedulerExt;
import com.netflix.titus.master.agent.service.AgentManagementConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;
import rx.Scheduler;
import rx.Single;
import rx.Subscription;
import rx.functions.Action0;
import rx.subjects.PublishSubject;

import static com.netflix.titus.master.MetricConstants.METRIC_AGENT_CACHE;
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
    private Subscription synchronizeWithInstanceCacheSubscription;

    private volatile AgentDataSnapshot dataSnapshot = new AgentDataSnapshot();

    private final PublishSubject<CacheUpdateEvent> eventSubject = PublishSubject.create();

    @Inject
    public DefaultAgentCache(AgentManagementConfiguration configuration,
                             AgentStore agentStore,
                             InstanceCloudConnector connector,
                             Registry registry) {
        this(configuration, agentStore, connector, registry, SchedulerExt.createSingleThreadScheduler("agent-cache"));
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

        logger.info("Started AgentCache with instance groups: {}", dataSnapshot.getInstanceGroups());

        this.instanceCacheSubscription = instanceCache.events()
                .subscribe(
                        event -> {
                            switch (event.getType()) {
                                case InstanceGroupAdded:
                                    onEventLoop(() -> updateOnInstanceGroupAddEvent(event.getResourceId()));
                                    break;
                                case InstanceGroupRemoved:
                                    onEventLoop(() -> updateOnInstanceGroupRemoveEvent(event.getResourceId()));
                                    break;
                                case InstanceGroupUpdated:
                                    onEventLoop(() -> updateOnInstanceGroupUpdateEvent(event.getResourceId()));
                                    break;
                            }
                        },
                        e -> logger.error("InstanceCache events stream completed with an error", e),
                        () -> logger.info("InstanceCache events stream completed")
                );

        this.synchronizeWithInstanceCacheSubscription = ObservableExt.schedule(
                METRIC_AGENT_CACHE, registry, "synchronizeWithInstanceCache",
                onEventLoopWithSubscription(this::synchronizeWithInstanceCache),
                0, configuration.getSynchronizeWithInstanceCacheIntervalMs(), TimeUnit.MILLISECONDS, scheduler
        ).subscribe(
                next -> next.ifPresent(throwable -> logger.warn("Synchronizing with instance cache failed with an error", throwable)),
                e -> logger.error("Synchronizing with instance cache terminated with an error", e),
                () -> logger.info("Synchronizing with instance cache terminated")
        );
    }

    public void shutdown() {
        if (instanceCache != null) {
            instanceCache.shutdown();
        }
        ObservableExt.safeUnsubscribe(instanceCacheSubscription, synchronizeWithInstanceCacheSubscription);
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
    public Single<AgentInstanceGroup> updateInstanceGroupStore(String instanceGroupId, Function<AgentInstanceGroup, AgentInstanceGroup> function) {
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
    public Single<AgentInstanceGroup> updateInstanceGroupStoreAndSyncCloud(String instanceGroupId, Function<AgentInstanceGroup, AgentInstanceGroup> function) {
        return updateInstanceGroupStore(instanceGroupId, function)
                .flatMap(ig -> {
                    instanceCache.refreshInstanceGroup(instanceGroupId);
                    return Single.just(ig);
                });
    }

    @Override
    public Single<AgentInstance> updateAgentInstanceStore(String instanceId, Function<AgentInstance, AgentInstance> function) {
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
    public Observable<CacheUpdateEvent> events() {
        return eventSubject.asObservable();
    }

    private void synchronizeWithInstanceCache() {
        Set<String> existingInstanceGroupIds = dataSnapshot.getInstanceGroupIds();
        Set<String> allInstanceGroupIds = instanceCache.getInstanceGroups().stream().map(InstanceGroup::getId).collect(Collectors.toSet());
        Set<String> newInstanceGroupIds = allInstanceGroupIds.stream().filter(id -> !existingInstanceGroupIds.contains(id)).collect(Collectors.toSet());
        Set<String> removedInstanceGroupIds = existingInstanceGroupIds.stream().filter(id -> !allInstanceGroupIds.contains(id)).collect(Collectors.toSet());
        Set<String> currentInstanceGroupIds = existingInstanceGroupIds.stream().filter(allInstanceGroupIds::contains).collect(Collectors.toSet());

        newInstanceGroupIds.forEach(this::updateOnInstanceGroupAddEvent);
        removedInstanceGroupIds.forEach(this::updateOnInstanceGroupRemoveEvent);
        currentInstanceGroupIds.forEach(this::updateOnInstanceGroupUpdateEvent);
    }

    private void updateOnInstanceGroupAddEvent(String instanceGroupId) {
        InstanceGroup instanceGroup = instanceCache.getInstanceGroup(instanceGroupId);
        AgentInstanceGroup existingInstanceGroup = dataSnapshot.getInstanceGroup(instanceGroupId);

        if (instanceGroup != null && existingInstanceGroup == null) {
            String instanceType = instanceGroup.getInstanceType();
            ResourceDimension instanceResourceDimension;
            try {
                instanceResourceDimension = connector.getInstanceTypeResourceDimension(instanceType);
            } catch (Exception e) {
                logger.warn("Cannot resolve resource dimension for instance type: {}", instanceType);
                instanceResourceDimension = ResourceDimension.empty();
            }

            AgentInstanceGroup agentInstanceGroup = DataConverters.toAgentInstanceGroup(instanceGroup, instanceResourceDimension);
            Set<AgentInstance> agentInstances = instanceGroup.getInstanceIds().stream()
                    .map(instanceCache::getAgentInstance)
                    .filter(Objects::nonNull)
                    .map(DataConverters::toAgentInstance)
                    .collect(Collectors.toCollection(() -> new TreeSet<>(AgentInstance.idComparator())));

            setDataSnapshot(dataSnapshot.updateInstanceGroup(agentInstanceGroup, agentInstances));
            storeEagerly(agentInstanceGroup);
            logger.info("Updated cache for added instance group: {} with instances: {}", instanceGroupId, agentInstances);
            eventSubject.onNext(new CacheUpdateEvent(CacheUpdateType.InstanceGroup, instanceGroupId));
        }
    }

    private void updateOnInstanceGroupRemoveEvent(String instanceGroupId) {
        InstanceGroup instanceGroup = instanceCache.getInstanceGroup(instanceGroupId);
        AgentInstanceGroup existingInstanceGroup = dataSnapshot.getInstanceGroup(instanceGroupId);

        if (instanceGroup == null && existingInstanceGroup != null) {
            setDataSnapshot(dataSnapshot.removeInstanceGroup(instanceGroupId));
            storeEagerlyWithRemoveFlag(existingInstanceGroup);
            logger.info("Updated cache for removed instance group: {}", instanceGroupId);
            eventSubject.onNext(new CacheUpdateEvent(CacheUpdateType.InstanceGroup, instanceGroupId));
        }
    }

    private void updateOnInstanceGroupUpdateEvent(String instanceGroupId) {
        InstanceGroup instanceGroup = instanceCache.getInstanceGroup(instanceGroupId);
        AgentInstanceGroup existingInstanceGroup = dataSnapshot.getInstanceGroup(instanceGroupId);

        if (instanceGroup != null && existingInstanceGroup != null) {
            Set<AgentInstance> existingAgentInstances = dataSnapshot.getInstances(instanceGroupId);

            AgentInstanceGroup agentInstanceGroup = DataConverters.updateAgentInstanceGroup(existingInstanceGroup, instanceGroup);
            Set<AgentInstance> agentInstances = instanceGroup.getInstanceIds().stream()
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
                    .collect(Collectors.toCollection(() -> new TreeSet<>(AgentInstance.idComparator())));

            if (isAgentInstanceGroupUpdated(existingInstanceGroup, agentInstanceGroup)
                    || isAgentInstancesUpdated(existingAgentInstances, agentInstances)) {
                setDataSnapshot(dataSnapshot.updateInstanceGroup(agentInstanceGroup, agentInstances));
                logger.info("Updated cache for updated instance group: {} with instances: {}", instanceGroupId, agentInstances);
                eventSubject.onNext(new CacheUpdateEvent(CacheUpdateType.InstanceGroup, instanceGroupId));
            }
        }
    }

    private void storeEagerly(AgentInstanceGroup instanceGroup) {
        if (instanceGroup != null) {
            agentStore.storeAgentInstanceGroup(instanceGroup).subscribe(
                    () -> logger.info("Persisted instance group: {} to the store", instanceGroup.getId()),
                    e -> logger.warn("Could not persist instance group: {} the store", instanceGroup.getId(), e)
            );
        }
    }

    private void storeEagerlyWithRemoveFlag(AgentInstanceGroup instanceGroup) {
        if (instanceGroup != null && !isTaggedToRemove(instanceGroup)) {
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

    private <T> Single<T> onEventLoopWithSubscription(Supplier<T> supplier) {
        return Single.create(subscriber -> {
            Subscription subscription = worker.schedule(() -> {
                try {
                    subscriber.onSuccess(supplier.get());
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

    private boolean isAgentInstanceGroupUpdated(AgentInstanceGroup first, AgentInstanceGroup second) {
        // equals cannot be used due to changing timestamps
        boolean notUpdated = first.getMin() == second.getMin() && first.getDesired() == second.getDesired() && first.getCurrent() == second.getCurrent()
                && first.getMax() == second.getMax() && first.isLaunchEnabled() == second.isLaunchEnabled()
                && first.isTerminateEnabled() == second.isTerminateEnabled() && Objects.equals(first.getId(), second.getId())
                && Objects.equals(first.getInstanceType(), second.getInstanceType()) && first.getTier() == second.getTier()
                && Objects.equals(first.getResourceDimension(), second.getResourceDimension())
                && Objects.equals(first.getLifecycleStatus(), second.getLifecycleStatus())
                && Objects.equals(first.getAttributes(), second.getAttributes());
        return !notUpdated;
    }

    private boolean isAgentInstancesUpdated(Set<AgentInstance> first, Set<AgentInstance> second) {
        // convert into HashSet because TreeSet does not use equals
        return !Sets.symmetricDifference(new HashSet<>(first), new HashSet<>(second)).isEmpty();
    }
}