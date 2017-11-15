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

package io.netflix.titus.master.agent.service.vm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.spectator.api.Registry;
import io.netflix.titus.api.agent.model.AgentInstance;
import io.netflix.titus.api.agent.model.AgentInstanceGroup;
import io.netflix.titus.api.agent.model.AutoScaleRule;
import io.netflix.titus.api.agent.service.AgentManagementException;
import io.netflix.titus.api.agent.store.AgentStore;
import io.netflix.titus.api.connector.cloud.Instance;
import io.netflix.titus.api.connector.cloud.InstanceCloudConnector;
import io.netflix.titus.api.connector.cloud.InstanceGroup;
import io.netflix.titus.api.model.ResourceDimension;
import io.netflix.titus.common.util.guice.annotation.Activator;
import io.netflix.titus.common.util.rx.ObservableExt;
import io.netflix.titus.master.agent.service.AgentManagementConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action0;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;

import static io.netflix.titus.master.agent.store.AgentStoreReaper.isTaggedToRemove;
import static io.netflix.titus.master.agent.store.AgentStoreReaper.tagToRemove;

/**
 * {@link DefaultAgentCache} merges data from a storage and the cloud provider. It creates also an event stream of agent
 * topology updates. It performs the following actions:
 * <ul>
 * <li>merges cloud provider {@link InstanceGroup} with {@link AgentInstanceGroup}</li>
 * <li>merges cloud provider {@link Instance} with {@link AgentInstance}</li>
 * <li>for a newly discovred {@link InstanceGroup} creates a default {@link AgentInstanceGroup} instance</li>
 * <li>for a newly discovred {@link Instance} creates a default {@link AgentInstance} instance</li>
 * <li>if a {@link InstanceGroup} or {@link Instance} is destroyed, removes it from a store after a configurable time passes</li>
 * <li>handles client updates in a thread safe way (runs all update tasks on an internal event loop)</li>
 * </ul>
 */
@Singleton
public class DefaultAgentCache implements AgentCache {

    private static final Logger logger = LoggerFactory.getLogger(DefaultAgentCache.class);

    private final Scheduler scheduler;

    private final AgentManagementConfiguration configuration;

    /**
     * Each newly discovered ASG will have this default auto scaling rule set.
     */
    private final AutoScaleRule defaultAutoScaleRule;

    private final AgentStore agentStore;
    private final InstanceCloudConnector connector;
    private final Scheduler.Worker worker;
    private final AgentCacheMetrics metrics;

    private VmServersCache vmServersCache;
    private Subscription vmServersCacheSubscription;

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
        this.defaultAutoScaleRule = AutoScaleRule.newBuilder()
                .withPriority(100)
                .withMinIdleToKeep(configuration.getAutoScaleRuleMinIdleToKeep())
                .withMaxIdleToKeep(configuration.getAutoScaleRuleMaxIdleToKeep())
                .withMin(configuration.getAutoScaleRuleMin())
                .withMax(configuration.getAutoScaleRuleMax())
                .withCoolDownSec(configuration.getAutoScaleRuleCoolDownSec())
                .withShortfallAdjustingFactor(configuration.getAutoScaleRuleShortfallAdjustingFactor())
                .build();
        this.configuration = configuration;
        this.agentStore = agentStore;
        this.connector = connector;
        this.scheduler = scheduler;
        this.worker = scheduler.createWorker();
        this.metrics = new AgentCacheMetrics(registry);
    }

    /**
     * The initialization process consists of the following steps:
     * <ul>
     * <li>Load all previously discovered instance groups and agent instances from the store</li>
     * <li>Create {@link VmServersCache} with the known instance groups, and synchronously try to refresh this data.</li>
     * <li>If {@link VmServersCache} is not initialized within configured time, discard it and create an empty one.</li>
     * <li>If {@link VmServersCache} initialization succeeds, merge the updates with the data loaded from the store</li>
     * <p>
     * </ul>
     */
    @Activator
    public void enterActiveMode() {
        List<AgentInstanceGroup> persistedServerGroups = agentStore.retrieveAgentInstanceGroups()
                .filter(g -> !isTaggedToRemove(g))
                .toList()
                .toBlocking()
                .first();
        List<AgentInstance> persistedServers = agentStore.retrieveAgentInstances().toList().toBlocking().first();

        Set<String> knownServerGroupIds = persistedServerGroups.stream().map(AgentInstanceGroup::getId).collect(Collectors.toSet());
        this.vmServersCache = VmServersCache.newInstance(configuration, connector, knownServerGroupIds, scheduler);
        setDataSnapshot(AgentDataSnapshot.initWithStaleDataSnapshot(persistedServerGroups, persistedServers));

        logger.info("Started AgentCache with: {}", dataSnapshot.getInstanceGroups());

        this.vmServersCacheSubscription = vmServersCache.events().subscribe(
                event -> {
                    switch (event.getType()) {
                        case Refreshed:
                            onEventLoop(this::updateOnFullRefreshVmCacheEvent);
                            break;
                        case ServerGroup:
                            onEventLoop(() -> updateOnServerGroupVmCacheEvent(event.getResourceId()));
                            break;
                        case Server:
                            // Ignore, as instance group, and its instances are refreshed at the same time
                            break;
                    }
                },
                e -> logger.error("VmServerCache events stream completed with an error", e),
                () -> logger.info("VmServerCache events stream completed")
        );

        updateOnFullRefreshVmCacheEvent();
    }

    public void shutdown() {
        if (vmServersCache != null) {
            vmServersCache.shutdown();
        }
        ObservableExt.safeUnsubscribe(vmServersCacheSubscription);
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
    public Set<AgentInstance> getAgentInstances(String instanceGroupId) {
        return AgentManagementException.checkInstanceGroupFound(dataSnapshot.getInstances(instanceGroupId), instanceGroupId);
    }

    @Override
    public AgentInstance getAgentInstance(String instanceId) {
        return AgentManagementException.checkAgentFound(dataSnapshot.getInstance(instanceId), instanceId);
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
            eventSubject.onNext(new CacheUpdateEvent(CacheUpdateType.ServerGroup, instanceGroup.getId()));
        }).concatWith(agentStore.storeAgentInstanceGroup(instanceGroup));
    }

    @Override
    public Completable updateInstanceGroupStoreAndSyncCloud(AgentInstanceGroup instanceGroup) {
        return updateInstanceGroupStore(instanceGroup).doOnCompleted(() -> vmServersCache.refreshServerGroup(instanceGroup.getId()));
    }

    @Override
    public Completable updateAgentInstanceStore(AgentInstance agentInstance) {
        return onEventLoopWithSubscription(() -> {
            getInstanceGroup(agentInstance.getInstanceGroupId());
            setDataSnapshot(dataSnapshot.updateAgentInstance(agentInstance));
            eventSubject.onNext(new CacheUpdateEvent(CacheUpdateType.Server, agentInstance.getId()));
        }).concatWith(agentStore.storeAgentInstance(agentInstance));
    }

    @Override
    public Completable removeInstances(String instanceGroupId, Set<String> agentInstanceIds) {
        return onEventLoopWithSubscription(() ->
                setDataSnapshot(dataSnapshot.removeInstances(instanceGroupId, agentInstanceIds))
        ).concatWith(agentStore.storeAgentInstanceGroup(dataSnapshot.getInstanceGroup(instanceGroupId)));
    }

    @Override
    public void forceRefresh() {
        vmServersCache.doFullServerGroupRefresh().subscribe(
                () -> logger.info("Forced full VM cache refresh finished"),
                e -> logger.info("Forced full VM cache refresh failed", e)
        );
    }

    @Override
    public Observable<CacheUpdateEvent> events() {
        return eventSubject;
    }

    private void updateOnFullRefreshVmCacheEvent() {
        Set<String> knownServerGroupIds = dataSnapshot.getInstanceGroupIds();
        List<InstanceGroup> allServerGroups = vmServersCache.getServerGroups();
        Set<String> allServerGroupIds = allServerGroups.stream().map(InstanceGroup::getId).collect(Collectors.toSet());

        Set<String> newServerGroupIds = allServerGroups.stream()
                .map(InstanceGroup::getId)
                .filter(id -> !knownServerGroupIds.contains(id))
                .collect(Collectors.toSet());
        Set<String> removedServerGroupIds = knownServerGroupIds.stream().filter(id -> !allServerGroupIds.contains(id)).collect(Collectors.toSet());
        List<AgentInstanceGroup> removedServerGroups = removedServerGroupIds.stream().map(id -> dataSnapshot.getInstanceGroup(id)).collect(Collectors.toList());

        newServerGroupIds.forEach(this::syncServerGroupWithVmCache);
        removedServerGroupIds.forEach(this::syncServerGroupWithVmCache);

        newServerGroupIds.forEach(this::storeEagerly);
        removedServerGroups.forEach(this::storeEagerlyWithRemoveFlag);
    }

    private void updateOnServerGroupVmCacheEvent(String instanceGroupId) {
        AgentInstanceGroup instanceGroup = getInstanceGroup(instanceGroupId);
        if (instanceGroup != null) {
            syncServerGroupWithVmCache(instanceGroupId);
            if (vmServersCache.getServerGroup(instanceGroupId) == null) {
                storeEagerlyWithRemoveFlag(instanceGroup);
            }
        }
    }

    private void syncServerGroupWithVmCache(String instanceGroupId) {
        InstanceGroup instanceGroup = vmServersCache.getServerGroup(instanceGroupId);

        // No longer exists in the cloud
        if (instanceGroup == null) {
            setDataSnapshot(dataSnapshot.removeInstanceGroup(instanceGroupId));
            eventSubject.onNext(new CacheUpdateEvent(CacheUpdateType.ServerGroup, instanceGroupId));
            return;
        }

        AgentInstanceGroup previous = dataSnapshot.getInstanceGroup(instanceGroupId);
        AgentInstanceGroup agentInstanceGroup;
        List<AgentInstance> agentInstances;
        if (previous == null) {
            String instanceType = instanceGroup.getAttributes().getOrDefault(VmServersCache.ATTR_INSTANCE_TYPE, "unknown");
            ResourceDimension instanceResourceDimension;
            try {
                instanceResourceDimension = connector.getInstanceTypeResourceDimension(instanceType);
            } catch (Exception e) {
                logger.warn("Cannot resolve resource dimension for instance type {}", instanceType);
                instanceResourceDimension = ResourceDimension.empty();
            }

            agentInstanceGroup = DataConverters.toAgentInstanceGroup(
                    instanceGroup,
                    instanceResourceDimension,
                    defaultAutoScaleRule
            );
            agentInstances = instanceGroup.getInstanceIds().stream()
                    .map(vmServersCache::getAgentInstance)
                    .filter(Objects::nonNull)
                    .map(DataConverters::toAgentInstance)
                    .collect(Collectors.toList());
        } else {
            agentInstanceGroup = DataConverters.updateAgentInstanceGroup(previous, instanceGroup);
            agentInstances = instanceGroup.getInstanceIds().stream()
                    .map(id -> {
                        Instance instance = vmServersCache.getAgentInstance(id);
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
        setDataSnapshot(dataSnapshot.updateInstanceGroup(agentInstanceGroup, agentInstanceSet));
        eventSubject.onNext(new CacheUpdateEvent(CacheUpdateType.ServerGroup, instanceGroupId));
    }

    private void storeEagerly(String instanceGroupId) {
        AgentInstanceGroup instanceGroup = dataSnapshot.getInstanceGroup(instanceGroupId);
        if (instanceGroup != null) {
            agentStore.storeAgentInstanceGroup(instanceGroup).subscribe(
                    () -> logger.info("Persisted instance group {} to the store", instanceGroup.getId()),
                    e -> logger.warn("Could not persist instance group {} the store", instanceGroup.getId(), e)
            );
        }
    }

    private void storeEagerlyWithRemoveFlag(AgentInstanceGroup instanceGroup) {
        if (!isTaggedToRemove(instanceGroup)) {
            agentStore.storeAgentInstanceGroup(tagToRemove(instanceGroup, scheduler)).subscribe(
                    () -> logger.info("Tagging instance group {} as removed", instanceGroup.getId()),
                    e -> logger.warn("Could not persist instance group {} into the store", instanceGroup.getId(), e)
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

    private void setDataSnapshot(AgentDataSnapshot dataSnapshot) {
        this.dataSnapshot = dataSnapshot;
        metrics.refresh(dataSnapshot);
    }
}
