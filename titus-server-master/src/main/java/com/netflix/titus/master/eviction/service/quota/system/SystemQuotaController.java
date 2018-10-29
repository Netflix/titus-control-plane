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
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.api.eviction.model.SystemDisruptionBudget;
import com.netflix.titus.api.model.TokenBucketPolicies;
import com.netflix.titus.common.runtime.SystemLogEvent;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.limiter.tokenbucket.TokenBucket;
import com.netflix.titus.common.util.rx.ReactorExt;
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

    private final TitusRuntime titusRuntime;

    private final Disposable resolverDisposable;

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

        this.systemTokenBucket = newTokenBucket(systemDisruptionBudgetResolver.resolve()
                .timeout(BOOTSTRAP_TIMEOUT)
                .blockFirst());
        this.resolverDisposable = systemDisruptionBudgetResolver.resolve()
                .compose(instrumentedRetryer("systemDisruptionBudgetResolver", retryInterval, logger))
                .subscribe(next -> this.systemTokenBucket = newTokenBucket(next));
    }

    @PreDestroy
    public void shutdown() {
        ReactorExt.safeDispose(resolverDisposable);
    }

    @Override
    public boolean consume(String taskId) {
        try {
            return systemTokenBucket.tryTake(1);
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    @Override
    public long getQuota() {
        return systemTokenBucket.getNumberOfTokens();
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
