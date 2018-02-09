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

package io.netflix.titus.master.agent.service.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import io.netflix.titus.api.agent.service.AgentManagementException;
import io.netflix.titus.api.connector.cloud.Instance;
import io.netflix.titus.api.connector.cloud.InstanceCloudConnector;
import io.netflix.titus.api.connector.cloud.InstanceGroup;
import io.netflix.titus.api.connector.cloud.InstanceLaunchConfiguration;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.common.util.rx.InstrumentedEventLoop;
import io.netflix.titus.common.util.rx.ObservableExt;
import io.netflix.titus.common.util.rx.RetryHandlerBuilder;
import io.netflix.titus.common.util.spectator.ContinuousSubscriptionMetrics;
import io.netflix.titus.common.util.tuple.Pair;
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

import static io.netflix.titus.common.util.spectator.SpectatorExt.continuousSubscriptionMetrics;
import static io.netflix.titus.master.MetricConstants.METRIC_AGENT_CACHE;
import static java.util.Arrays.asList;
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
class InstanceCache {

    private static final Logger logger = LoggerFactory.getLogger(InstanceCache.class);

    static String ATTR_INSTANCE_TYPE = "instanceType";

    private static final long BOOT_TIMEOUT_MS = 60_000;
    private static final int BOOT_RETRY_COUNT = 10;
    private static final long BOOT_RETRY_DELAYS_MS = 1_000;
    private static final long MAX_REFRESH_TIMEOUT = 600_000;

    private final AgentManagementConfiguration configuration;
    private final InstanceCloudConnector connector;
    private final Registry registry;
    private final InstrumentedEventLoop eventLoop;

    private volatile InstanceCacheDataSnapshot cacheSnapshot;

    private final Subscription fullInstanceGroupRefreshSubscription;
    private final Subscription instanceGroupRefreshSubscription;

    private final PublishSubject<CacheUpdateEvent> eventSubject = PublishSubject.create();

    private ContinuousSubscriptionMetrics fullInstanceGroupRefreshMetricsTransformer;
    private Map<String, ContinuousSubscriptionMetrics> instanceGroupRefreshMetricsTransformers = new ConcurrentHashMap<>();

    private InstanceCache(AgentManagementConfiguration configuration,
                          InstanceCloudConnector connector,
                          Set<String> knownInstanceGroups,
                          Registry registry,
                          Scheduler scheduler) {
        this.configuration = configuration;
        this.connector = connector;
        this.registry = registry;
        this.cacheSnapshot = InstanceCacheDataSnapshot.empty();
        this.eventLoop = ObservableExt.createEventLoop(METRIC_AGENT_CACHE + "eventLoop", registry, scheduler);

        List<Tag> tags = Collections.singletonList(new BasicTag("class", InstanceCache.class.getSimpleName()));
        fullInstanceGroupRefreshMetricsTransformer = continuousSubscriptionMetrics(METRIC_AGENT_CACHE + "fullInstanceGroupRefresh", tags, registry);

        // Synchronously refresh information about the known instance groups
        List<Completable> initialRefresh = knownInstanceGroups.stream().map(this::doInstanceGroupRefresh).collect(Collectors.toList());
        Throwable error = Completable.merge(initialRefresh).timeout(BOOT_TIMEOUT_MS, TimeUnit.MILLISECONDS).retryWhen(RetryHandlerBuilder.retryHandler()
                .withRetryCount(BOOT_RETRY_COUNT)
                .withDelay(BOOT_RETRY_DELAYS_MS, BOOT_RETRY_DELAYS_MS, TimeUnit.MILLISECONDS)
                .withScheduler(scheduler)
                .buildExponentialBackoff()
        ).get();

        if (error != null) {
            throw AgentManagementException.initializationError("Cannot extract instance group data from the cloud (known instance group ids: %s)", error, knownInstanceGroups);
        }

        this.fullInstanceGroupRefreshSubscription = ObservableExt.schedule(
                METRIC_AGENT_CACHE, registry, "doFullInstanceGroupRefresh", doFullInstanceGroupRefresh(),
                0, configuration.getFullCacheRefreshIntervalMs(), TimeUnit.MILLISECONDS, scheduler
        ).subscribe(
                next -> next.ifPresent(throwable -> logger.warn("Full refresh cycle failed with an error", throwable)),
                e -> logger.error("Full cache refresh process terminated with an error", e),
                () -> logger.info("Full cache refresh process terminated")
        );

        this.instanceGroupRefreshSubscription = ObservableExt.schedule(
                METRIC_AGENT_CACHE, registry, "doInstanceGroupRefresh", doInstanceGroupRefresh(),
                0, configuration.getCacheRefreshIntervalMs(), TimeUnit.MILLISECONDS, scheduler
        ).subscribe(
                next -> next.ifPresent(throwable -> logger.warn("Instance group refresh cycle failed with an error", throwable)),
                e -> logger.error("Instance group cache refresh process terminated with an error", e),
                () -> logger.info("Instance group cache refresh process terminated")
        );

    }

    void shutdown() {
        eventLoop.shutdown();
        fullInstanceGroupRefreshSubscription.unsubscribe();
        instanceGroupRefreshSubscription.unsubscribe();
        fullInstanceGroupRefreshMetricsTransformer.remove();
    }

    List<InstanceGroup> getInstanceGroups() {
        return cacheSnapshot.getInstanceGroups();
    }

    InstanceGroup getInstanceGroup(String id) {
        return cacheSnapshot.getInstanceGroup(id);
    }

    Instance getAgentInstance(String id) {
        return cacheSnapshot.getAgentInstance(id);
    }

    /**
     * Trigger refresh request, but without returning {@link Completable} to the caller. Instead subscribe eagerly.
     * If refresh fails, it will eventually succeed on its regular update cycle.
     */
    void refreshInstanceGroup(String instanceGroupId) {
        InstanceGroup instanceGroup = cacheSnapshot.getInstanceGroup(instanceGroupId);
        if (instanceGroup != null) {
            doInstanceGroupRefresh(instanceGroup).subscribe();
        }
    }

    Observable<CacheUpdateEvent> events() {
        return eventSubject;
    }

    /**
     * Loads all known instance groups, to discover the newly created ones.
     */
    Completable doFullInstanceGroupRefresh() {
        Completable completable = connector.getInstanceGroups()
                .flatMap(unfiltered ->
                        getInstanceGroupPattern()
                                .map(p -> Observable.just(findOurInstanceGroups(p, unfiltered)))
                                .orElse(Observable.empty()))
                .flatMap(newInstanceGroups -> {
                    Set<String> allKnownIds = cacheSnapshot.getInstanceGroups().stream().map(InstanceGroup::getId).collect(Collectors.toSet());
                    Set<String> allDiscoveredIds = newInstanceGroups.stream().map(InstanceGroup::getId).collect(Collectors.toSet());
                    Set<String> newArrivalIds = CollectionsExt.copyAndRemove(allDiscoveredIds, allKnownIds);

                    logger.debug("Performing full instance group refresh: knownIds={}, foundIds={}, newIds={}",
                            allKnownIds, allDiscoveredIds, newArrivalIds);

                    if (newArrivalIds.isEmpty()) {
                        return Observable.empty();
                    }

                    logger.info("Performing full instance group refresh: newIds={}", newArrivalIds);

                    Map<String, InstanceGroup> newArrivalsById = newInstanceGroups.stream()
                            .filter(instanceGroup -> newArrivalIds.contains(instanceGroup.getId()))
                            .collect(Collectors.toMap(InstanceGroup::getId, Function.identity()));

                    Map<String, InstanceGroup> newArrivalsByLaunchConfigId = newArrivalsById.values().stream()
                            .filter(instanceGroup -> !Strings.isNullOrEmpty(instanceGroup.getLaunchConfigurationName()))
                            .collect(Collectors.toMap(InstanceGroup::getLaunchConfigurationName, Function.identity()));

                    List<String> launchConfigIds = new ArrayList<>(newArrivalsByLaunchConfigId.keySet());

                    return connector.getInstanceLaunchConfiguration(launchConfigIds).map(launchConfigurations ->
                            launchConfigurations.stream()
                                    .map(launchConfiguration -> {
                                        InstanceGroup instanceGroup = newArrivalsByLaunchConfigId.get(launchConfiguration.getId());
                                        if (instanceGroup != null) {
                                            return Pair.of(launchConfiguration, instanceGroup);
                                        }
                                        return null;
                                    })
                                    .filter(Objects::nonNull)
                                    .map(pair -> decorate(pair.getRight(), pair.getLeft()))
                                    .collect(Collectors.toList())
                    );
                })
                .doOnNext(instanceGroups ->
                        onEventLoop("addInstanceGroups", () -> {
                            this.cacheSnapshot = cacheSnapshot.addInstanceGroups(instanceGroups);
                            eventSubject.onNext(new CacheUpdateEvent(CacheUpdateType.Refreshed, CacheUpdateEvent.EMPTY_ID));

                            instanceGroups.forEach(instanceGroup -> refreshInstanceGroup(instanceGroup.getId()));
                        })
                ).toCompletable();

        return completable.compose(fullInstanceGroupRefreshMetricsTransformer.asCompletable())
                .timeout(MAX_REFRESH_TIMEOUT, TimeUnit.MILLISECONDS);
    }

    private Completable.Transformer getInstanceGroupRefreshMetricsTransform(String instanceGroupId) {
        return instanceGroupRefreshMetricsTransformers.computeIfAbsent(instanceGroupId, k -> {
            List<Tag> tags = asList(
                    new BasicTag("class", InstanceCache.class.getSimpleName()),
                    new BasicTag("instanceGroupId", instanceGroupId)
            );
            return continuousSubscriptionMetrics(METRIC_AGENT_CACHE + "instanceGroupRefresh", tags, registry);
        }).asCompletable();
    }

    private Optional<Pattern> getInstanceGroupPattern() {
        String patternValue = configuration.getAgentInstanceGroupPattern();
        try {
            return Optional.of(Pattern.compile(patternValue));
        } catch (Exception e) {
            logger.warn("Cannot parse agent instance group name pattern: {}. New instance groups discovery not possible", patternValue);
            return Optional.empty();
        }
    }

    private List<InstanceGroup> findOurInstanceGroups(Pattern pattern, List<InstanceGroup> unfiltered) {
        return unfiltered.stream().filter(instanceGroup -> pattern.matcher(instanceGroup.getId()).matches()).collect(Collectors.toList());
    }

    private Completable doInstanceGroupRefresh() {
        return ObservableExt.fromCallable(() -> cacheSnapshot.getInstanceGroups())
                .flatMap(instanceGroup -> doInstanceGroupRefresh(instanceGroup).toObservable())
                .toCompletable();
    }

    private Completable doInstanceGroupRefresh(InstanceGroup instanceGroup) {
        return doInstanceGroupRefresh(instanceGroup.getId());
    }

    /**
     * Refreshes single instance group, and its instances. Updates cache and emits corresponding events.
     * Never emits error, which is instead logged.
     */
    private Completable doInstanceGroupRefresh(String instanceGroupId) {
        Observable<Void> updateAction = connector.getInstanceGroups(singletonList(instanceGroupId))
                .flatMap(result -> {
                    if (result.isEmpty()) {
                        onEventLoop("removeInstanceGroup", () -> {
                            removeInstanceGroup(instanceGroupId);
                            logger.info("Instance group: {} has been removed", instanceGroupId);
                        });
                        return Observable.empty();
                    }

                    InstanceGroup instanceGroup = result.get(0);

                    return connector.getInstancesByInstanceGroupId(instanceGroup.getId())
                            .doOnNext(updatedInstances -> onEventLoop("updateInstances", () -> {
                                // update the instance ids on the instance group
                                List<String> instanceIds = updatedInstances.stream().map(Instance::getId).sorted().collect(Collectors.toList());
                                InstanceGroup updatedInstanceGroup = instanceGroup.toBuilder().withInstanceIds(instanceIds).build();
                                updateCache(updatedInstanceGroup, updatedInstances);
                            }))
                            .ignoreElements()
                            .cast(Void.class);
                });

        Completable completable = updateAction.materialize().take(1).doOnNext(
                result -> {
                    if (result.getKind() == Notification.Kind.OnError) {
                        logger.warn("Instance group: {} refresh error", instanceGroupId, result.getThrowable());
                    }
                }
        ).toCompletable();

        return completable.compose(getInstanceGroupRefreshMetricsTransform(instanceGroupId))
                .timeout(MAX_REFRESH_TIMEOUT, TimeUnit.MILLISECONDS);
    }

    private void updateCache(InstanceGroup updatedInstanceGroup, List<Instance> updatedInstances) {
        String instanceGroupId = updatedInstanceGroup.getId();
        InstanceGroup oldInstanceGroup = cacheSnapshot.getInstanceGroup(instanceGroupId);

        if (oldInstanceGroup == null) {
            this.cacheSnapshot = cacheSnapshot.updateInstanceGroup(updatedInstanceGroup);
            this.cacheSnapshot = cacheSnapshot.updateInstances(updatedInstances);
            eventSubject.onNext(new CacheUpdateEvent(CacheUpdateType.InstanceGroup, instanceGroupId));
            return;
        }

        InstanceGroup effectiveInstanceGroup = decorate(updatedInstanceGroup, oldInstanceGroup);
        boolean instanceGroupChanged = !oldInstanceGroup.equals(effectiveInstanceGroup);
        boolean instancesChanged = oldInstanceGroup.getInstanceIds().size() != updatedInstanceGroup.getInstanceIds().size();

        if (!instancesChanged) {
            for (Instance newInstance : updatedInstances) {
                Instance oldInstance = cacheSnapshot.getAgentInstance(newInstance.getId());
                if (oldInstance == null || !oldInstance.equals(newInstance)) {
                    instancesChanged = true;
                    break;
                }
            }
        }

        if (instanceGroupChanged) {
            logger.info("Refreshed cache state due to instance group: {} update", instanceGroupId);
            this.cacheSnapshot = cacheSnapshot.updateInstanceGroup(effectiveInstanceGroup);
        }
        if (instancesChanged) {
            logger.info("Refreshed cache state due to instance count/state update in instance group: {}", instanceGroupId);
            this.cacheSnapshot = cacheSnapshot.updateInstances(updatedInstances);
        }
        if (instanceGroupChanged || instancesChanged) {
            eventSubject.onNext(new CacheUpdateEvent(CacheUpdateType.InstanceGroup, instanceGroupId));
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

    private InstanceGroup decorate(InstanceGroup newInstanceGroup, InstanceGroup oldInstanceGroup) {
        Map<String, String> newAttributes = CollectionsExt.copyAndAdd(
                newInstanceGroup.getAttributes(),
                ATTR_INSTANCE_TYPE, oldInstanceGroup.getAttributes().get(ATTR_INSTANCE_TYPE)
        );
        return newInstanceGroup.toBuilder().withAttributes(newAttributes).build();
    }

    private void removeInstanceGroup(String removedInstanceGroupId) {
        this.cacheSnapshot = cacheSnapshot.removeInstanceGroup(removedInstanceGroupId);
        eventSubject.onNext(new CacheUpdateEvent(CacheUpdateType.InstanceGroup, removedInstanceGroupId));
        ContinuousSubscriptionMetrics transformer = instanceGroupRefreshMetricsTransformers.remove(removedInstanceGroupId);
        if (transformer != null) {
            transformer.remove();
        }
    }

    private void onEventLoop(String actionName, Action0 action) {
        eventLoop.schedule(actionName, () -> {
            try {
                action.call();
            } catch (Exception e) {
                logger.warn("InstanceCache actionName: {} internal operation error", actionName, e);
                throw e;
            }
        });
    }

    static InstanceCache newInstance(AgentManagementConfiguration configuration,
                                     InstanceCloudConnector connector,
                                     Set<String> knownInstanceGroups,
                                     Registry registry,
                                     Scheduler scheduler) {
        return new InstanceCache(configuration, connector, knownInstanceGroups, registry, scheduler);
    }
}
