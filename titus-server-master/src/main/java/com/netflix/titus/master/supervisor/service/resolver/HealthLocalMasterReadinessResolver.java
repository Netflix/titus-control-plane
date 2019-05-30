/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.master.supervisor.service.resolver;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.runtime.health.api.HealthCheckAggregator;
import com.netflix.titus.api.supervisor.model.MasterInstance;
import com.netflix.titus.api.supervisor.model.ReadinessState;
import com.netflix.titus.api.supervisor.model.ReadinessStatus;
import com.netflix.titus.api.supervisor.service.LocalMasterReadinessResolver;
import com.netflix.titus.api.supervisor.service.resolver.PollingLocalMasterReadinessResolver;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.retry.Retryers;
import com.netflix.titus.common.util.time.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * Alters {@link MasterInstance} state based on instance health status.
 */
@Singleton
public class HealthLocalMasterReadinessResolver implements LocalMasterReadinessResolver {

    private static final Logger logger = LoggerFactory.getLogger(HealthLocalMasterReadinessResolver.class);

    private static final ScheduleDescriptor REFRESH_SCHEDULER_DESCRIPTOR = ScheduleDescriptor.newBuilder()
            .withName(HealthLocalMasterReadinessResolver.class.getSimpleName())
            .withDescription("Local Master state adapter")
            .withInitialDelay(Duration.ZERO)
            .withInterval(Duration.ofSeconds(5))
            .withRetryerSupplier(() -> Retryers.exponentialBackoff(100, 1_000, TimeUnit.MILLISECONDS, 5))
            .withTimeout(Duration.ofSeconds(5))
            .withOnErrorHandler((action, error) -> {
                logger.warn("Cannot refresh health status: {}", error.getMessage());
                logger.debug(error.getMessage(), error);
            })
            .build();

    private final HealthCheckAggregator healthCheckAggregator;
    private final Clock clock;

    private final PollingLocalMasterReadinessResolver poller;

    @Inject
    public HealthLocalMasterReadinessResolver(HealthCheckAggregator healthCheckAggregator,
                                              TitusRuntime titusRuntime) {
        this(healthCheckAggregator, REFRESH_SCHEDULER_DESCRIPTOR, titusRuntime, Schedulers.parallel());
    }

    public HealthLocalMasterReadinessResolver(HealthCheckAggregator healthCheckAggregator,
                                              ScheduleDescriptor scheduleDescriptor,
                                              TitusRuntime titusRuntime,
                                              Scheduler scheduler) {
        this.healthCheckAggregator = healthCheckAggregator;
        this.clock = titusRuntime.getClock();
        this.poller = PollingLocalMasterReadinessResolver.newPollingResolver(
                refresh(),
                scheduleDescriptor,
                titusRuntime,
                scheduler
        );
    }

    @PreDestroy
    public void shutdown() {
        poller.shutdown();
    }

    @Override
    public Flux<ReadinessStatus> observeLocalMasterReadinessUpdates() {
        return poller.observeLocalMasterReadinessUpdates();
    }

    private Mono<ReadinessStatus> refresh() {
        return Mono.fromFuture(healthCheckAggregator.check())
                .map(health -> {
                    ReadinessStatus.Builder builder = ReadinessStatus.newBuilder().withTimestamp(clock.wallTime());
                    if (health.isHealthy()) {
                        builder.withState(ReadinessState.Enabled).withMessage("Instance is healthy");
                    } else {
                        builder.withState(ReadinessState.Disabled).withMessage("Instance is unhealthy: " + health.getHealthResults());
                    }
                    return builder.build();
                });
    }
}
