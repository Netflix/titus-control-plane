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

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.api.model.FixedIntervalTokenBucketRefillPolicy;
import com.netflix.titus.api.model.TokenBucketRefillPolicy;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.master.MetricConstants;

class SystemQuotaMetrics {

    private static final String ROOT_NAME = MetricConstants.METRIC_SCHEDULING_EVICTION + "systemQuota.";

    private final Registry registry;

    private final Id tokenBucketCapacityId;
    private final Id tokenBucketRefillRateId;
    private final Id quotaLevelId;

    SystemQuotaMetrics(SystemQuotaController systemQuotaController, TitusRuntime titusRuntime) {
        this.registry = titusRuntime.getRegistry();

        this.tokenBucketCapacityId = registry.createId(ROOT_NAME + "tokenBucket.capacity");
        PolledMeter.using(registry)
                .withId(tokenBucketCapacityId)
                .monitorValue(systemQuotaController, sqc -> systemQuotaController.getDisruptionBudget().getTokenBucketPolicy().getCapacity());
        this.tokenBucketRefillRateId = registry.createId(ROOT_NAME + "tokenBucket.refillRatePerSec");
        PolledMeter.using(registry)
                .withId(tokenBucketRefillRateId)
                .monitorValue(systemQuotaController, sqc -> {
                    TokenBucketRefillPolicy refillPolicy = systemQuotaController.getDisruptionBudget().getTokenBucketPolicy().getRefillPolicy();
                    if (refillPolicy instanceof FixedIntervalTokenBucketRefillPolicy) {
                        FixedIntervalTokenBucketRefillPolicy fixed = (FixedIntervalTokenBucketRefillPolicy) refillPolicy;
                        return fixed.getNumberOfTokensPerInterval() / (fixed.getIntervalMs() / 1000D);
                    }
                    return 0;
                });

        this.quotaLevelId = registry.createId(ROOT_NAME + "available");
        PolledMeter.using(registry)
                .withId(quotaLevelId)
                .monitorValue(systemQuotaController, sqc -> systemQuotaController.getQuota());
    }

    void shutdown() {
        PolledMeter.remove(registry, tokenBucketCapacityId);
        PolledMeter.remove(registry, tokenBucketRefillRateId);
        PolledMeter.remove(registry, quotaLevelId);
    }
}
