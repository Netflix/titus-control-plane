/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.master.kubernetes.controller;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import javax.annotation.PreDestroy;

import com.netflix.spectator.api.Gauge;
import com.netflix.titus.common.framework.scheduler.LocalScheduler;
import com.netflix.titus.common.framework.scheduler.ScheduleReference;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.ExecutorsExt;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.guice.annotation.Deactivator;
import com.netflix.titus.common.util.limiter.Limiters;
import com.netflix.titus.common.util.limiter.tokenbucket.FixedIntervalTokenBucketConfiguration;
import com.netflix.titus.common.util.limiter.tokenbucket.TokenBucket;
import com.netflix.titus.master.MetricConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseGcController<T> {
    private static final Logger logger = LoggerFactory.getLogger(BaseGcController.class);

    protected final String name;
    protected final String description;
    protected final TitusRuntime titusRuntime;
    protected final LocalScheduler scheduler;
    protected final FixedIntervalTokenBucketConfiguration tokenBucketConfiguration;
    protected final ControllerConfiguration controllerConfiguration;

    protected String metricRoot;
    protected ExecutorService executorService;
    protected ScheduleReference schedulerRef;
    protected TokenBucket tokenBucket;

    protected final Gauge skippedGauge;
    protected final Gauge successesGauge;
    protected final Gauge failuresGauge;

    public BaseGcController(
            String name,
            String description,
            TitusRuntime titusRuntime,
            LocalScheduler scheduler,
            FixedIntervalTokenBucketConfiguration tokenBucketConfiguration,
            ControllerConfiguration controllerConfiguration
    ) {
        this.name = name;
        this.description = description;
        this.titusRuntime = titusRuntime;
        this.scheduler = scheduler;

        this.metricRoot = MetricConstants.METRIC_KUBERNETES_CONTROLLER + name;

        this.skippedGauge = titusRuntime.getRegistry().gauge(metricRoot, "type", "skipped");
        this.successesGauge = titusRuntime.getRegistry().gauge(metricRoot, "type", "successes");
        this.failuresGauge = titusRuntime.getRegistry().gauge(metricRoot, "type", "failures");
        this.tokenBucketConfiguration = tokenBucketConfiguration;
        this.controllerConfiguration = controllerConfiguration;
    }

    @Activator
    public void enterActiveMode() {
        ScheduleDescriptor gcScheduleDescriptor = ScheduleDescriptor.newBuilder()
                .withName(name)
                .withDescription(description)
                .withInitialDelay(Duration.ofMillis(controllerConfiguration.getControllerInitialDelayMs()))
                .withInterval(Duration.ofMillis(controllerConfiguration.getControllerIntervalMs()))
                .withTimeout(Duration.ofMillis(controllerConfiguration.getControllerTimeoutMs()))
                .build();

        executorService = ExecutorsExt.namedSingleThreadExecutor(name);
        schedulerRef = scheduler.schedule(gcScheduleDescriptor, e -> doGc(), executorService);

        tokenBucket = Limiters.createInstrumentedFixedIntervalTokenBucket(
                name + "TokenBucket",
                tokenBucketConfiguration,
                currentTokenBucket -> logger.info("Token bucket: {} configuration updated with: {}", name, currentTokenBucket),
                titusRuntime
        );
    }

    @Deactivator
    @PreDestroy
    public void shutdown() {
        Evaluators.acceptNotNull(executorService, ExecutorService::shutdown);
        Evaluators.acceptNotNull(schedulerRef, ScheduleReference::cancel);
        resetGauges();
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public String getMetricRoot() {
        return metricRoot;
    }

    private void doGc() {
        if (!controllerConfiguration.isControllerEnabled() || !shouldGc()) {
            logger.info("Skipping gc execution for: {}", name);
            resetGauges();
            return;
        }

        List<T> allItemsToGc = Collections.emptyList();
        try {
            allItemsToGc = getItemsToGc();
        } catch (Exception e) {
            logger.error("Unable to get items to GC due to:", e);
        }

        int total = allItemsToGc.size();
        int limitedNumberOfItemsToGc = (int) Math.min(total, tokenBucket.getNumberOfTokens());
        int skipped = total - limitedNumberOfItemsToGc;
        int successes = 0;
        int failures = 0;

        if (limitedNumberOfItemsToGc > 0 && tokenBucket.tryTake(limitedNumberOfItemsToGc)) {
            List<T> itemsToGc = allItemsToGc.subList(0, limitedNumberOfItemsToGc);
            logger.debug("Attempting to GC: {}", itemsToGc);
            for (T item : itemsToGc) {
                try {
                    if (gcItem(item)) {
                        successes++;
                    } else {
                        failures++;
                    }
                } catch (Exception e) {
                    failures++;
                    logger.error("Unable to GC: {} due to:", item, e);
                }
            }
        }
        setGauges(skipped, successes, failures);
        logger.info("Finished GC iteration total:{}, skipped: {}, successes: {}, failures: {}", total,
                skipped, successes, failures);
    }

    public abstract boolean shouldGc();

    public abstract List<T> getItemsToGc();

    public abstract boolean gcItem(T item);

    private void setGauges(int skipped, int successes, int failures) {
        skippedGauge.set(skipped);
        successesGauge.set(successes);
        failuresGauge.set(failures);
    }

    private void resetGauges() {
        setGauges(0, 0, 0);
    }
}
