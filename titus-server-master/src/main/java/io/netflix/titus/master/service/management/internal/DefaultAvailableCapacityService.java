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

package io.netflix.titus.master.service.management.internal;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.spectator.api.Registry;
import io.netflix.titus.api.model.ResourceDimension;
import io.netflix.titus.api.model.Tier;
import io.netflix.titus.common.util.guice.annotation.Activator;
import io.netflix.titus.common.util.spectator.ExecutionMetrics;
import io.netflix.titus.master.MetricConstants;
import io.netflix.titus.master.agent.ServerInfo;
import io.netflix.titus.master.agent.service.server.ServerInfoResolver;
import io.netflix.titus.master.model.ResourceDimensions;
import io.netflix.titus.master.service.management.AvailableCapacityService;
import io.netflix.titus.master.service.management.CapacityAllocationService;
import io.netflix.titus.master.service.management.CapacityManagementConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Scheduler;
import rx.schedulers.Schedulers;

@Singleton
public class DefaultAvailableCapacityService implements AvailableCapacityService {

    private static final Logger logger = LoggerFactory.getLogger(DefaultAvailableCapacityService.class);

    private static final String METRIC_AVAILABLE_CAPACITY_UPDATE = MetricConstants.METRIC_CAPACITY_MANAGEMENT + "availableCapacityUpdate";

    private final CapacityManagementConfiguration configuration;
    private final CapacityAllocationService capacityAllocationService;
    private final ServerInfoResolver serverInfoResolver;

    private final Scheduler.Worker worker;
    private final ExecutionMetrics metrics;

    private volatile Map<Tier, ResourceDimension> availableCapacity = Collections.emptyMap();

    @Inject
    public DefaultAvailableCapacityService(CapacityManagementConfiguration configuration,
                                           CapacityAllocationService capacityAllocationService,
                                           ServerInfoResolver serverInfoResolver,
                                           Registry registry) {
        // FIXME This class contains blocking call, which have to be converted into proper observable chain.
        this(configuration, capacityAllocationService, serverInfoResolver, registry, Schedulers.io());
    }

    /* Visible for testing */ DefaultAvailableCapacityService(CapacityManagementConfiguration configuration,
                                                              CapacityAllocationService capacityAllocationService,
                                                              ServerInfoResolver serverInfoResolver,
                                                              Registry registry,
                                                              Scheduler scheduler) {
        this.configuration = configuration;
        this.capacityAllocationService = capacityAllocationService;
        this.serverInfoResolver = serverInfoResolver;
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
        List<String> tierInstanceTypes = ConfigUtil.getTierInstanceTypes(tier, configuration);

        List<Integer> limits = capacityAllocationService.limits(tierInstanceTypes)
                .timeout(60, TimeUnit.SECONDS)
                .toBlocking()
                .first();
        ResourceDimension total = ResourceDimension.empty();
        for (int i = 0; i < limits.size(); i++) {
            Optional<ServerInfo> serverInfo = serverInfoResolver.resolve(tierInstanceTypes.get(i));
            if (serverInfo.isPresent()) {
                total = ResourceDimensions.add(total, toResourceDimension(serverInfo.get(), limits.get(i)));
            }
        }
        return total;
    }

    private ResourceDimension toResourceDimension(ServerInfo serverInfo, int maxSize) {
        if (serverInfo.getGpus() > 0) {
            // Exclude GPU-enabled instances, as they are special case.
            return ResourceDimension.empty();
        }
        return new ResourceDimension(
                serverInfo.getCpus() * maxSize,
                serverInfo.getGpus() * maxSize,
                serverInfo.getMemoryGB() * 1024 * maxSize,
                serverInfo.getStorageGB() * 1024 * maxSize,
                serverInfo.getNetworkMbs() * maxSize
        );
    }
}
