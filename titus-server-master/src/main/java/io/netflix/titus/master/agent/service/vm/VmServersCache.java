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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.netflix.titus.api.agent.service.AgentManagementException;
import io.netflix.titus.api.connector.cloud.Instance;
import io.netflix.titus.api.connector.cloud.InstanceCloudConnector;
import io.netflix.titus.api.connector.cloud.InstanceGroup;
import io.netflix.titus.api.connector.cloud.InstanceLaunchConfiguration;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.common.util.rx.ObservableExt;
import io.netflix.titus.master.agent.service.AgentManagementConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Notification;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action0;
import rx.subjects.PublishSubject;

import static java.util.Collections.singletonList;

/**
 * Supplementary class, which periodically updates agent instance group/instance data from the cloud provider.
 * <h1>Periodic data refresh strategy</h1>
 * All data are refresh periodically:
 * <ul>
 * <li>Each known instance group (including instances) is refreshed every {@link AgentManagementConfiguration#getCacheRefreshIntervalMs()}</li>
 * <li>List of known instance groups is refreshed every {@link AgentManagementConfiguration#getFullCacheRefreshIntervalMs()} ()}</li>
 * </ul>
 */
class VmServersCache {

    private static final Logger logger = LoggerFactory.getLogger(VmServersCache.class);

    static String ATTR_INSTANCE_TYPE = "instanceType";

    private static final long BOOT_TIMEOUT_MS = 60_000;

    private final AgentManagementConfiguration configuration;
    private final InstanceCloudConnector connector;
    private final Scheduler.Worker worker;

    private volatile VmCacheDataSnapshot cacheSnapshot;

    private final Subscription fullServerGroupRefreshSubscription;
    private final Subscription instanceGroupRefreshSubscription;

    private final PublishSubject<CacheUpdateEvent> eventSubject = PublishSubject.create();

    private VmServersCache(AgentManagementConfiguration configuration, InstanceCloudConnector connector, Set<String> knownServerGroups, Scheduler scheduler) {
        this.configuration = configuration;
        this.connector = connector;
        this.cacheSnapshot = VmCacheDataSnapshot.empty();
        this.worker = scheduler.createWorker();

        // Synchronously refresh information about the known instance groups
        List<Completable> initialRefresh = knownServerGroups.stream().map(this::doServerGroupRefresh).collect(Collectors.toList());
        Throwable error = Completable.merge(initialRefresh).timeout(BOOT_TIMEOUT_MS, TimeUnit.MILLISECONDS).get();
        if (error != null) {
            throw AgentManagementException.initializationError("Cannot extract instance group data from the cloud (known instance group ids: %s)", error, knownServerGroups);
        }

        this.fullServerGroupRefreshSubscription = ObservableExt.schedule(
                doFullServerGroupRefresh(), 0, configuration.getFullCacheRefreshIntervalMs(), TimeUnit.MILLISECONDS, scheduler
        ).subscribe(
                next -> next.ifPresent(throwable -> logger.warn("Full refresh cycle failed with an error", throwable)),
                e -> logger.error("Full cache refresh process terminated with an error", e),
                () -> logger.info("Full cache refresh process terminated")
        );

        this.instanceGroupRefreshSubscription = ObservableExt.schedule(
                doServerGroupRefresh(), 0, configuration.getCacheRefreshIntervalMs(), TimeUnit.MILLISECONDS, scheduler
        ).subscribe(
                next -> next.ifPresent(throwable -> logger.warn("Server group refresh cycle failed with an error", throwable)),
                e -> logger.error("Server group cache refresh process terminated with an error", e),
                () -> logger.info("Server group cache refresh process terminated")
        );
    }

    void shutdown() {
        worker.unsubscribe();
        fullServerGroupRefreshSubscription.unsubscribe();
        instanceGroupRefreshSubscription.unsubscribe();
    }

    List<InstanceGroup> getServerGroups() {
        return cacheSnapshot.getInstanceGroups();
    }

    InstanceGroup getServerGroup(String id) {
        return cacheSnapshot.getServerGroup(id);
    }

    Instance getAgentInstance(String id) {
        return cacheSnapshot.getAgentInstance(id);
    }

    /**
     * Trigger refresh request, but without returning {@link Completable} to the caller. Instead subscriber eagerly.
     * If refresh fails, it will eventually succeeded on its regular update cycle.
     */
    void refreshServerGroup(String instanceGroupId) {
        InstanceGroup instanceGroup = cacheSnapshot.getServerGroup(instanceGroupId);
        if (instanceGroup != null) {
            doServerGroupRefresh(instanceGroup).subscribe();
        }
    }

    Observable<CacheUpdateEvent> events() {
        return eventSubject;
    }

    /**
     * Loads all known instance groups, to discover the newly created ones.
     */
    Completable doFullServerGroupRefresh() {
        return connector.getInstanceGroups()
                .flatMap(unfiltered ->
                        getServerGroupPattern()
                                .map(p -> Observable.just(findOurServerGroups(p, unfiltered)))
                                .orElse(Observable.empty()))
                .flatMap(newServerGroups -> {
                    Set<String> allKnownIds = cacheSnapshot.getInstanceGroups().stream().map(InstanceGroup::getId).collect(Collectors.toSet());
                    Set<String> allDiscoveredIds = newServerGroups.stream().map(InstanceGroup::getId).collect(Collectors.toSet());
                    Set<String> newArrivalIds = CollectionsExt.copyAndRemove(allDiscoveredIds, allKnownIds);

                    logger.info("Performing full instance group refresh: known={}, found={}, new={}",
                            allKnownIds.size(), allDiscoveredIds.size(), newArrivalIds.size());

                    if (newArrivalIds.isEmpty()) {
                        return Observable.empty();
                    }

                    Map<String, InstanceGroup> newArrivalsById = newServerGroups.stream()
                            .filter(sg -> newArrivalIds.contains(sg.getId()))
                            .collect(Collectors.toMap(InstanceGroup::getId, Function.identity()));

                    Map<String, InstanceGroup> newArrivalsByLaunchConfigId = newArrivalsById.values().stream()
                            .collect(Collectors.toMap(InstanceGroup::getLaunchConfigurationName, Function.identity()));
                    List<String> launchConfigIds = new ArrayList<>(newArrivalsByLaunchConfigId.keySet());

                    return connector.getInstanceLaunchConfiguration(launchConfigIds).map(launchConfigurations ->
                            launchConfigurations.stream()
                                    .map(lc -> decorate(newArrivalsByLaunchConfigId.get(lc.getId()), lc))
                                    .collect(Collectors.toList())
                    );
                })
                .doOnNext(instanceGroups ->
                        onEventLoop(() -> {
                            this.cacheSnapshot = cacheSnapshot.addServerGroups(instanceGroups);
                            eventSubject.onNext(new CacheUpdateEvent(CacheUpdateType.Refreshed, CacheUpdateEvent.EMPTY_ID));

                            instanceGroups.forEach(ig -> refreshServerGroup(ig.getId()));
                        })
                ).toCompletable();
    }

    private Optional<Pattern> getServerGroupPattern() {
        String patternValue = configuration.getAgentServerGroupPattern();
        try {
            return Optional.of(Pattern.compile(patternValue));
        } catch (Exception e) {
            logger.warn("Cannot parse agent instance group name pattern: {}. New instance groups discovery not possible", patternValue);
            return Optional.empty();
        }
    }

    private List<InstanceGroup> findOurServerGroups(Pattern pattern, List<InstanceGroup> unfiltered) {
        return unfiltered.stream().filter(instanceGroup -> pattern.matcher(instanceGroup.getId()).matches()).collect(Collectors.toList());
    }

    private Completable doServerGroupRefresh() {
        return ObservableExt.fromCallable(() -> cacheSnapshot.getInstanceGroups())
                .flatMap(vmServerGroup -> doServerGroupRefresh(vmServerGroup).toObservable())
                .toCompletable();
    }

    private Completable doServerGroupRefresh(InstanceGroup instanceGroup) {
        return doServerGroupRefresh(instanceGroup.getId());
    }

    /**
     * Refreshes single instance group, and its instances. Updates cache and emits corresponding events.
     * Never emits error, which is instead logged.
     */
    private Completable doServerGroupRefresh(String instanceGroupId) {
        Observable<Void> updateAction = connector.getInstanceGroups(singletonList(instanceGroupId))
                .flatMap(result -> {
                    if (result.isEmpty()) {
                        logger.info("Server group {} has been removed", instanceGroupId);
                        onEventLoop(() -> removeServerGroupFromCache(instanceGroupId));
                        return Observable.empty();
                    }

                    InstanceGroup updatedServerGroup = result.get(0);
                    return connector.getInstances(updatedServerGroup.getInstanceIds())
                            .doOnNext(updatedVmServers -> onEventLoop(() -> updateCache(updatedServerGroup, updatedVmServers)))
                            .ignoreElements()
                            .cast(Void.class);
                });

        return updateAction.materialize().take(1).doOnNext(
                result -> {
                    if (result.getKind() == Notification.Kind.OnError) {
                        logger.warn("Server group {} refresh error", instanceGroupId, result.getThrowable());
                    }
                }
        ).toCompletable();
    }

    private void updateCache(InstanceGroup updatedInstanceGroup, List<Instance> updatedInstances) {
        String instanceGroupId = updatedInstanceGroup.getId();
        InstanceGroup oldServerGroup = cacheSnapshot.getServerGroup(instanceGroupId);

        if (oldServerGroup == null) {
            this.cacheSnapshot = cacheSnapshot.updateServerGroup(updatedInstanceGroup);
            this.cacheSnapshot = cacheSnapshot.updateServers(updatedInstances);
            eventSubject.onNext(new CacheUpdateEvent(CacheUpdateType.ServerGroup, instanceGroupId));
            return;
        }

        InstanceGroup effectiveServerGroup = decorate(updatedInstanceGroup, oldServerGroup);
        boolean instanceGroupChanged = !oldServerGroup.equals(effectiveServerGroup);
        boolean instancesChanged = oldServerGroup.getInstanceIds().size() != updatedInstanceGroup.getInstanceIds().size();
        if (!instancesChanged) {
            for (Instance newServer : updatedInstances) {
                Instance oldServer = cacheSnapshot.getAgentInstance(newServer.getId());
                if (oldServer == null || !oldServer.equals(newServer)) {
                    instancesChanged = true;
                    break;
                }
            }
        }
        if (instanceGroupChanged) {
            logger.info("Refreshed cache state due to instance group {} update", instanceGroupId);
            this.cacheSnapshot = cacheSnapshot.updateServerGroup(effectiveServerGroup);
        }
        if (instancesChanged) {
            logger.info("Refreshed cache state due to instance count/state update in instance group {}", instanceGroupId);
            this.cacheSnapshot = cacheSnapshot.updateServers(updatedInstances);
        }
        if (instanceGroupChanged || instancesChanged) {
            eventSubject.onNext(new CacheUpdateEvent(CacheUpdateType.ServerGroup, instanceGroupId));
        }
    }

    private InstanceGroup decorate(InstanceGroup instanceGroup, InstanceLaunchConfiguration launchConfiguration) {
        Map<String, String> newAttributes = CollectionsExt.copyAndAdd(
                instanceGroup.getAttributes(),
                ATTR_INSTANCE_TYPE, launchConfiguration.getInstanceType()
        );
        return instanceGroup.toBuilder()
                .withAttributes(newAttributes)
                .build();
    }

    private InstanceGroup decorate(InstanceGroup newServerGroup, InstanceGroup oldServerGroup) {
        Map<String, String> newAttributes = CollectionsExt.copyAndAdd(
                newServerGroup.getAttributes(),
                ATTR_INSTANCE_TYPE, oldServerGroup.getAttributes().get(ATTR_INSTANCE_TYPE)
        );
        return newServerGroup.toBuilder().withAttributes(newAttributes).build();
    }

    private void removeServerGroupFromCache(String removedServerGroupId) {
        this.cacheSnapshot = cacheSnapshot.removeServerGroup(removedServerGroupId);
        eventSubject.onNext(new CacheUpdateEvent(CacheUpdateType.ServerGroup, removedServerGroupId));
    }

    private void onEventLoop(Action0 action) {
        worker.schedule(() -> {
            try {
                action.call();
            } catch (Exception e) {
                logger.warn("VmServerCache internal operation error", e);
            }
        });
    }

    static VmServersCache newInstance(AgentManagementConfiguration configuration,
                                      InstanceCloudConnector connector,
                                      Set<String> knownServerGroups,
                                      Scheduler scheduler) {
        return new VmServersCache(configuration, connector, knownServerGroups, scheduler);
    }
}
