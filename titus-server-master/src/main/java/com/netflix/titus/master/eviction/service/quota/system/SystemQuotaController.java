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

package com.netflix.titus.master.eviction.service.quota.system;

import java.time.Duration;
import java.util.Collections;
import java.util.function.Supplier;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.api.eviction.model.EvictionQuota;
import com.netflix.titus.api.eviction.model.SystemDisruptionBudget;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.TimeWindowFunctions;
import com.netflix.titus.api.model.TokenBucketPolicies;
import com.netflix.titus.api.model.reference.Reference;
import com.netflix.titus.common.runtime.SystemLogEvent;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.limiter.tokenbucket.TokenBucket;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.master.eviction.service.quota.ConsumptionResult;
import com.netflix.titus.master.eviction.service.quota.QuotaController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;

import static com.netflix.titus.common.util.rx.ReactorRetriers.instrumentedRetryer;

@Singleton
public class SystemQuotaController implements QuotaController<Void> {

    private static final Logger logger = LoggerFactory.getLogger(SystemQuotaController.class);

    private static final Duration BOOTSTRAP_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration DEFAULT_RETRY_INTERVAL = Duration.ofSeconds(5);

    private static final String NAME = "system";

    private static final ConsumptionResult QUOTA_LIMIT_EXCEEDED = ConsumptionResult.rejected("System eviction quota limit exceeded");
    private static final ConsumptionResult OUTSIDE_SYSTEM_TIME_WINDOW = ConsumptionResult.rejected("Outside system time window");

    private final TitusRuntime titusRuntime;

    private final SystemQuotaMetrics metrics;
    private final Disposable resolverDisposable;

    private volatile SystemDisruptionBudget disruptionBudget;
    private volatile String quotaMessage;

    private volatile Supplier<Boolean> inTimeWindowPredicate;
    private volatile TokenBucket systemTokenBucket;

    @Inject
    public SystemQuotaController(SystemDisruptionBudgetResolver systemDisruptionBudgetResolver,
                                 TitusRuntime titusRuntime) {
        this(systemDisruptionBudgetResolver, DEFAULT_RETRY_INTERVAL, titusRuntime);
    }

    @VisibleForTesting
    SystemQuotaController(SystemDisruptionBudgetResolver systemDisruptionBudgetResolver,
                          Duration retryInterval,
                          TitusRuntime titusRuntime) {
        this.titusRuntime = titusRuntime;

        this.disruptionBudget = systemDisruptionBudgetResolver.resolve().timeout(BOOTSTRAP_TIMEOUT).blockFirst();
        resetDisruptionBudget(disruptionBudget);

        this.metrics = new SystemQuotaMetrics(this, titusRuntime);

        this.resolverDisposable = systemDisruptionBudgetResolver.resolve()
                .compose(instrumentedRetryer("systemDisruptionBudgetResolver", retryInterval, logger))
                .subscribe(next -> {
                    try {
                        resetDisruptionBudget(next);
                    } catch (Exception e) {
                        logger.warn("Invalid system disruption budget configuration: ", e);
                    }
                });
    }

    private void resetDisruptionBudget(SystemDisruptionBudget newDisruptionBudget) {
        this.disruptionBudget = newDisruptionBudget;
        this.systemTokenBucket = newTokenBucket(newDisruptionBudget);
        this.inTimeWindowPredicate = TimeWindowFunctions.isInTimeWindowPredicate(titusRuntime, disruptionBudget.getTimeWindows());
        this.quotaMessage = String.format("System quota token bucket: capacity=%s, refillRate=%s",
                systemTokenBucket.getCapacity(),
                systemTokenBucket.getRefillStrategy()
        );
    }

    @PreDestroy
    public void shutdown() {
        metrics.shutdown();
        ReactorExt.safeDispose(resolverDisposable);
    }

    @Override
    public ConsumptionResult consume(String taskId) {
        try {
            if (!inTimeWindowPredicate.get()) {
                return OUTSIDE_SYSTEM_TIME_WINDOW;
            }
            return systemTokenBucket.tryTake(1)
                    ? ConsumptionResult.approved()
                    : QUOTA_LIMIT_EXCEEDED;
        } catch (IllegalArgumentException e) {
            return QUOTA_LIMIT_EXCEEDED;
        }
    }

    @Override
    public void giveBackConsumedQuota(String taskId) {
        systemTokenBucket.refill(1);
    }

    @Override
    public EvictionQuota getQuota(Reference reference) {
        EvictionQuota.Builder quotaBuilder = EvictionQuota.newBuilder().withReference(reference);
        if (!inTimeWindowPredicate.get()) {
            return quotaBuilder.withQuota(0).withMessage(OUTSIDE_SYSTEM_TIME_WINDOW.getRejectionReason().get()).build();
        }
        if (systemTokenBucket.getNumberOfTokens() <= 0) {
            return quotaBuilder.withQuota(0).withMessage(QUOTA_LIMIT_EXCEEDED.getRejectionReason().get()).build();
        }
        return quotaBuilder.withQuota(systemTokenBucket.getNumberOfTokens()).withMessage(quotaMessage).build();
    }

    /**
     * Exposed for metrics collection (see {@link SystemQuotaMetrics}).
     */
    SystemDisruptionBudget getDisruptionBudget() {
        return disruptionBudget;
    }

    private TokenBucket newTokenBucket(SystemDisruptionBudget disruptionBudget) {
        logger.info("Configuring new system disruption budget: {}", disruptionBudget);

        titusRuntime.getSystemLogService().submit(SystemLogEvent.newBuilder()
                .withComponent("eviction")
                .withPriority(SystemLogEvent.Priority.Info)
                .withMessage("System disruption budget update: " + disruptionBudget)
                .withCategory(SystemLogEvent.Category.Other)
                .withTags(Collections.singleton("eviction"))
                .build()
        );

        return TokenBucketPolicies.newTokenBucket(NAME, disruptionBudget.getTokenBucketPolicy());
    }
}
