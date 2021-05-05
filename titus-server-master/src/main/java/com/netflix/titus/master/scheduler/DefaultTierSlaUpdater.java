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

package com.netflix.titus.master.scheduler;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.fenzo.queues.tiered.TieredQueueSlas;
import com.netflix.fenzo.sla.ResAllocs;
import com.netflix.fenzo.sla.ResAllocsBuilder;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.util.rx.SchedulerExt;
import com.netflix.titus.master.model.ResourceDimensions;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import com.netflix.titus.master.service.management.AvailableCapacityService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;

import static com.netflix.titus.api.model.SchedulerConstants.SCHEDULER_NAME_FENZO;

@Singleton
public class DefaultTierSlaUpdater implements TierSlaUpdater {

    private static final Logger logger = LoggerFactory.getLogger(DefaultTierSlaUpdater.class);

    private final SchedulerConfiguration schedulerConfiguration;
    private final ApplicationSlaManagementService applicationSlaManagementService;
    private final AvailableCapacityService availableCapacityService;
    private final Scheduler scheduler;

    @Inject
    public DefaultTierSlaUpdater(SchedulerConfiguration schedulerConfiguration,
                                 ApplicationSlaManagementService applicationSlaManagementService,
                                 AvailableCapacityService availableCapacityService) {
        this(schedulerConfiguration, applicationSlaManagementService, availableCapacityService, SchedulerExt.createSingleThreadScheduler("tier-sla-updater"));
    }

    /**
     * Visible for testing
     */
    DefaultTierSlaUpdater(SchedulerConfiguration schedulerConfiguration,
                          ApplicationSlaManagementService applicationSlaManagementService,
                          AvailableCapacityService availableCapacityService,
                          Scheduler scheduler) {
        this.schedulerConfiguration = schedulerConfiguration;
        this.applicationSlaManagementService = applicationSlaManagementService;
        this.availableCapacityService = availableCapacityService;
        this.scheduler = scheduler;
    }

    @Override
    public Observable<TieredQueueSlas> tieredQueueSlaUpdates() {
        return Observable.interval(0, schedulerConfiguration.getTierSlaUpdateIntervalMs(), TimeUnit.MILLISECONDS, scheduler)
                .flatMap(tick -> {
                    try {
                        return Observable.just(recomputeSLAs());
                    } catch (Exception e) {
                        logger.warn("Failed to recompute tier SLAs", e);
                        return Observable.empty();
                    }
                }).share();
    }

    private TieredQueueSlas recomputeSLAs() {
        Collection<ApplicationSLA> applicationSLAs = applicationSlaManagementService.getApplicationSLAsForScheduler(SCHEDULER_NAME_FENZO);

        Map<Integer, ResAllocs> tierCapacities = new HashMap<>();
        Map<Integer, Map<String, ResAllocs>> slas = new HashMap<>();

        // Tiers
        for (Tier tier : Tier.values()) {
            Optional<ResourceDimension> tierCapacity = availableCapacityService.totalCapacityOf(tier);
            tierCapacity.ifPresent(rd -> tierCapacities.put(tier.ordinal(), ResourceDimensions.toResAllocs(tier.name(), rd)));
        }

        // Capacity groups
        for (ApplicationSLA applicationSLA : applicationSLAs) {
            Map<String, ResAllocs> tierCapacityGroups = slas.computeIfAbsent(applicationSLA.getTier().ordinal(), n -> new HashMap<>());
            tierCapacityGroups.put(applicationSLA.getAppName(), toResAllocs(applicationSLA));
        }

        logger.debug("Resolved tier capacities: {}", tierCapacities);
        logger.debug("Resolved capacity group resource guarantees: {}", slas);

        return new MyTieredQueueSlas(tierCapacities, slas);
    }

    static ResAllocs toResAllocs(ApplicationSLA applicationSLA) {
        int instanceCount = applicationSLA.getInstanceCount();
        ResourceDimension instanceDimension = applicationSLA.getResourceDimension();
        return new ResAllocsBuilder(applicationSLA.getAppName())
                .withCores(instanceDimension.getCpu() * instanceCount)
                .withMemory(instanceDimension.getMemoryMB() * instanceCount)
                .withDisk(instanceDimension.getDiskMB() * instanceCount)
                .withNetworkMbps(instanceDimension.getNetworkMbs() * instanceCount)
                .build();
    }

    /**
     * {@link TieredQueueSlas} does not provide any means to verify its state after construction. Solely for testing
     * purposes we provide this extension.
     */
    static class MyTieredQueueSlas extends TieredQueueSlas {

        private final Map<Integer, ResAllocs> tierCapacities;
        private final Map<Integer, Map<String, ResAllocs>> groupCapacities;

        MyTieredQueueSlas(Map<Integer, ResAllocs> tierCapacities, Map<Integer, Map<String, ResAllocs>> groupCapacities) {
            super(tierCapacities, groupCapacities);
            this.tierCapacities = tierCapacities;
            this.groupCapacities = groupCapacities;
        }

        Map<Integer, ResAllocs> getTierCapacities() {
            return tierCapacities;
        }

        Map<Integer, Map<String, ResAllocs>> getGroupCapacities() {
            return groupCapacities;
        }
    }
}
