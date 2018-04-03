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

package com.netflix.titus.master.service.management.internal;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.util.guice.ProxyType;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.guice.annotation.ProxyConfiguration;
import com.netflix.titus.common.util.rx.ComputationTaskInvoker;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.service.management.CapacityGuaranteeStrategy;
import com.netflix.titus.master.service.management.CapacityManagementConfiguration;
import com.netflix.titus.master.service.management.CapacityMonitoringService;
import com.netflix.titus.master.store.ApplicationSlaStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
import rx.Subscription;
import rx.schedulers.Schedulers;

@Singleton
@ProxyConfiguration(types = ProxyType.ActiveGuard)
public class DefaultCapacityMonitoringService implements CapacityMonitoringService {

    private static final Logger logger = LoggerFactory.getLogger(DefaultCapacityMonitoringService.class);

    // An upper bound on the update execution time.
    @VisibleForTesting
    static final long UPDATE_TIMEOUT_MS = 30 * 1000;

    /**
     * Schedule periodically capacity dimensioning updates, to override any changes made by external actors.
     * (for example, a user doing manual override).
     */
    @VisibleForTesting
    static final long PERIODIC_UPDATE_INTERVAL_MS = 5 * 60 * 1000;

    private final CapacityManagementConfiguration configuration;
    private final CapacityGuaranteeStrategy strategy;

    private final ApplicationSlaStore storage;

    private final Scheduler scheduler;
    private final AgentManagementService agentManagementService;
    private final ComputationTaskInvoker<Void> invoker;
    private final DefaultCapacityMonitoringServiceMetrics metrics;

    private Subscription periodicUpdateSubscription;

    public DefaultCapacityMonitoringService(CapacityManagementConfiguration configuration,
                                            CapacityGuaranteeStrategy strategy,
                                            ApplicationSlaStore storage,
                                            AgentManagementService agentManagementService,
                                            Registry registry,
                                            Scheduler scheduler) {
        this.configuration = configuration;
        this.strategy = strategy;
        this.storage = storage;
        this.agentManagementService = agentManagementService;
        this.invoker = new ComputationTaskInvoker<>(Observable.unsafeCreate((Observable.OnSubscribe<Void>) this::updateAction), scheduler);
        this.scheduler = scheduler;
        this.metrics = new DefaultCapacityMonitoringServiceMetrics(registry);
    }

    @Inject
    public DefaultCapacityMonitoringService(CapacityManagementConfiguration configuration,
                                            CapacityGuaranteeStrategy strategy,
                                            ApplicationSlaStore storage,
                                            AgentManagementService agentManagementService,
                                            Registry registry) {
        this(configuration, strategy, storage, agentManagementService, registry, Schedulers.computation());
    }

    @PreDestroy
    public void shutdown() {
        if (periodicUpdateSubscription != null) {
            periodicUpdateSubscription.unsubscribe();
        }
    }

    @Activator
    public Observable<Void> enterActiveMode() {
        logger.info("Entering active mode");
        schedulePeriodicUpdate();
        return Observable.empty();
    }

    @Override
    public Observable<Void> refresh() {
        return invoker.recompute();
    }

    private void schedulePeriodicUpdate() {
        this.periodicUpdateSubscription = Observable.interval(0, PERIODIC_UPDATE_INTERVAL_MS, TimeUnit.MILLISECONDS, scheduler)
                .subscribe(tick ->
                        invoker.recompute()
                                .subscribe(
                                        none -> {
                                        },
                                        e -> logger.error("Scheduled capacity guarantees refresh failed: {}", e.getMessage()),
                                        () -> logger.debug("Scheduled capacity guarantees refresh finished")
                                )
                );
    }

    private void updateAction(Subscriber<? super Void> result) {
        try {
            long startTime = scheduler.now();

            // Compute capacity allocations for all tiers, and for those tiers that are scaled (now only Critical).
            // We need full capacity allocation for alerting.
            Observable<Pair<CapacityGuaranteeStrategy.CapacityAllocations, CapacityGuaranteeStrategy.CapacityAllocations>> allocationsObservable = storage.findAll().toList()
                    .map(allSlas -> Pair.of(recompute(allSlas), recompute(scalableSLAs(allSlas))));

            Observable<Void> updateStatus = allocationsObservable.flatMap(allocationPair -> {
                CapacityGuaranteeStrategy.CapacityAllocations allTiersAllocation = allocationPair.getLeft();
                CapacityGuaranteeStrategy.CapacityAllocations scaledAllocations = allocationPair.getRight();

                metrics.recordResourceShortage(allTiersAllocation);

                List<Observable<Void>> updates = new ArrayList<>();
                scaledAllocations.getInstanceGroups().forEach(instanceGroup -> {
                            int minSize = scaledAllocations.getExpectedMinSize(instanceGroup);
                            updates.add(agentManagementService.updateCapacity(instanceGroup.getId(), Optional.of(minSize), Optional.empty()).toObservable());
                        }
                );
                return Observable.merge(updates);
            });

            updateStatus
                    .timeout(UPDATE_TIMEOUT_MS, TimeUnit.MILLISECONDS, scheduler)
                    .subscribe(
                            never -> {
                            },
                            e -> {
                                metrics.getUpdateExecutionMetrics().failure(e, startTime);
                                logger.error("Capacity update failure", e);
                                result.onError(e);
                            },
                            () -> {
                                metrics.getUpdateExecutionMetrics().success(startTime);
                                logger.debug("Capacity guarantee changes applied");
                                result.onCompleted();
                            }
                    );
        } catch (Throwable e) {
            result.onError(e);
            throw e;
        }
    }

    private List<ApplicationSLA> scalableSLAs(List<ApplicationSLA> allSLAs) {
        return allSLAs.stream().filter(sla -> sla.getTier() == Tier.Critical).collect(Collectors.toList());
    }

    private CapacityGuaranteeStrategy.CapacityAllocations recompute(List<ApplicationSLA> allSLAs) {
        EnumMap<Tier, List<ApplicationSLA>> tiers = new EnumMap<>(Tier.class);
        allSLAs.forEach(sla -> {
            List<ApplicationSLA> tierSLAs = tiers.computeIfAbsent(sla.getTier(), k -> new ArrayList<>());
            tierSLAs.add(sla);
        });
        CapacityGuaranteeStrategy.CapacityAllocations allocations = strategy.compute(new CapacityGuaranteeStrategy.CapacityRequirements(tiers));
        logger.debug("Recomputed resource dimensions: {}", allocations);

        return allocations;
    }
}