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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.rx.SchedulerExt;
import com.netflix.titus.common.util.spectator.ExecutionMetrics;
import com.netflix.titus.master.MetricConstants;
import com.netflix.titus.master.agent.ServerInfo;
import com.netflix.titus.master.agent.service.server.ServerInfoResolver;
import com.netflix.titus.master.model.ResourceDimensions;
import com.netflix.titus.master.service.management.AvailableCapacityService;
import com.netflix.titus.master.service.management.CapacityManagementConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Scheduler;

import static com.netflix.titus.master.service.management.CapacityManagementFunctions.isAvailableToUse;

@Singleton
public class DefaultAvailableCapacityService implements AvailableCapacityService {

    private static final Logger logger = LoggerFactory.getLogger(DefaultAvailableCapacityService.class);

    private static final String METRIC_AVAILABLE_CAPACITY_UPDATE = MetricConstants.METRIC_CAPACITY_MANAGEMENT + "availableCapacityUpdate";

    private final CapacityManagementConfiguration configuration;
    private final ServerInfoResolver serverInfoResolver;

    private final Scheduler.Worker worker;
    private final ExecutionMetrics metrics;
    private final AgentManagementService agentManagementService;

    private volatile Map<Tier, ResourceDimension> availableCapacity = Collections.emptyMap();

    @Inject
    public DefaultAvailableCapacityService(CapacityManagementConfiguration configuration,
                                           ServerInfoResolver serverInfoResolver,
                                           AgentManagementService agentManagementService,
                                           Registry registry) {
        this(configuration, serverInfoResolver, agentManagementService, registry, SchedulerExt.createSingleThreadScheduler("available-capacity-service"));
    }

    @VisibleForTesting
    DefaultAvailableCapacityService(CapacityManagementConfiguration configuration,
                                    ServerInfoResolver serverInfoResolver,
                                    AgentManagementService agentManagementService,
                                    Registry registry,
                                    Scheduler scheduler) {
        this.configuration = configuration;
        this.serverInfoResolver = serverInfoResolver;
        this.agentManagementService = agentManagementService;
        this.worker = scheduler.createWorker();
        this.metrics = new ExecutionMetrics(METRIC_AVAILABLE_CAPACITY_UPDATE, DefaultAvailableCapacityService.class, registry);
    }

    @Activator
    public void enterActiveMode() {
        update();
        scheduleUpdate(configuration.getAvailableCapacityUpdateIntervalMs());
    }

    @PreDestroy
    public void shutdown() {
        worker.unsubscribe();
    }

    @Override
    public Optional<ResourceDimension> totalCapacityOf(Tier tier) {
        return Optional.ofNullable(availableCapacity.get(tier));
    }

    private void scheduleUpdate(long delayMs) {
        worker.schedule(() -> {
            logger.debug("Updating available tier capacity data...");
            update();
            scheduleUpdate(configuration.getAvailableCapacityUpdateIntervalMs());
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    private void update() {
        long startTime = worker.now();
        try {
            Map<Tier, ResourceDimension> availableCapacity = new HashMap<>();
            for (Tier tier : Tier.values()) {
                availableCapacity.put(tier, resolveCapacityOf(tier));
            }
            this.availableCapacity = availableCapacity;
            metrics.success(startTime);
            logger.debug("Resolved available capacity: {}", availableCapacity);
        } catch (Exception e) {
            logger.warn("Cannot resolve tier capacities", e);
            metrics.failure(e, startTime);
        }
    }

    private ResourceDimension resolveCapacityOf(Tier tier) {
        ResourceDimension total = ResourceDimension.empty();
        for (AgentInstanceGroup instanceGroup : agentManagementService.getInstanceGroups()) {
            if (instanceGroup.getTier() == tier && isAvailableToUse(agentManagementService, instanceGroup)) {
                Optional<ServerInfo> serverInfo = serverInfoResolver.resolve(instanceGroup.getInstanceType());
                if (serverInfo.isPresent()) {
                    long maxSize = tier == Tier.Critical ? instanceGroup.getCurrent() : instanceGroup.getMax();
                    total = ResourceDimensions.add(total, toResourceDimension(serverInfo.get(), maxSize));
                }
            }
        }
        return total;
    }

    private ResourceDimension toResourceDimension(ServerInfo serverInfo, long maxSize) {
        if (serverInfo.getGpus() > 0) {
            // Exclude GPU-enabled instances, as they are special case.
            return ResourceDimension.empty();
        }
        return new ResourceDimension(
                serverInfo.getCpus() * maxSize,
                serverInfo.getGpus() * maxSize,
                serverInfo.getMemoryGB() * 1024 * maxSize,
                serverInfo.getStorageGB() * 1024 * maxSize,
                serverInfo.getNetworkMbs() * maxSize,
                0);
    }
}