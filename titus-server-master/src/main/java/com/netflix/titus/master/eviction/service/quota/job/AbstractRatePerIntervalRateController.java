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

package com.netflix.titus.master.eviction.service.quota.job;

import com.netflix.titus.api.eviction.model.EvictionQuota;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.model.reference.Reference;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.DateTimeExt;
import com.netflix.titus.common.util.histogram.RollingCount;
import com.netflix.titus.master.eviction.service.quota.ConsumptionResult;
import com.netflix.titus.master.eviction.service.quota.QuotaController;

public abstract class AbstractRatePerIntervalRateController implements QuotaController<Job<?>> {

    private static final int RESOLUTION = 20;

    private final long intervalMs;
    private final long evictionsPerInterval;

    private final RollingCount rollingCount;
    private final ConsumptionResult rejectionResult;

    private final TitusRuntime titusRuntime;

    protected AbstractRatePerIntervalRateController(Job<?> job,
                                                    long intervalMs,
                                                    long evictionsPerInterval,
                                                    TitusRuntime titusRuntime) {
        this.intervalMs = intervalMs;
        this.evictionsPerInterval = evictionsPerInterval;
        this.titusRuntime = titusRuntime;

        this.rollingCount = RollingCount.rollingWindow(intervalMs, RESOLUTION, titusRuntime.getClock().wallTime());
        this.rejectionResult = buildRejectionResult(job, intervalMs, evictionsPerInterval);
    }

    protected AbstractRatePerIntervalRateController(Job<?> newJob,
                                                  long intervalMs,
                                                  long evictionsPerInterval,
                                                  AbstractRatePerIntervalRateController previous) {

        this.intervalMs = intervalMs;
        this.evictionsPerInterval = evictionsPerInterval;
        this.titusRuntime = previous.titusRuntime;

        if (intervalMs == previous.intervalMs) {
            this.rollingCount = previous.rollingCount;
        } else {
            long now = titusRuntime.getClock().wallTime();

            this.rollingCount = RollingCount.rollingWindow(intervalMs, RESOLUTION, titusRuntime.getClock().wallTime());
            rollingCount.add(Math.min(evictionsPerInterval, previous.rollingCount.getCounts(now)), now);
        }
        this.rejectionResult = buildRejectionResult(newJob, intervalMs, evictionsPerInterval);
    }

    @Override
    public EvictionQuota getQuota(Reference reference) {
        long quota = getQuota(titusRuntime.getClock().wallTime());
        return quota > 0
                ? EvictionQuota.newBuilder().withReference(reference).withQuota(quota).withMessage("Per interval limit %s", evictionsPerInterval).build()
                : EvictionQuota.newBuilder().withReference(reference).withQuota(0).withMessage(rejectionResult.getRejectionReason().get()).build();
    }

    @Override
    public ConsumptionResult consume(String taskId) {
        long now = titusRuntime.getClock().wallTime();

        if (getQuota(now) >= 1) {
            rollingCount.addOne(now);
            return ConsumptionResult.approved();
        }
        return rejectionResult;
    }

    @Override
    public void giveBackConsumedQuota(String taskId) {
        rollingCount.add(-1, titusRuntime.getClock().wallTime());
    }

    private ConsumptionResult buildRejectionResult(Job<?> job, long intervalMs, long rate) {
        return ConsumptionResult.rejected(String.format(
                "Exceeded the number of tasks that can be evicted in %s (limit=%s, rate limiter type=%s)",
                DateTimeExt.toTimeUnitString(intervalMs),
                rate,
                job.getJobDescriptor().getDisruptionBudget().getDisruptionBudgetRate().getClass().getSimpleName()
        ));
    }

    private int getQuota(long now) {
        long terminatedInLastInterval = rollingCount.getCounts(now);
        return (int) (evictionsPerInterval - terminatedInLastInterval);
    }
}

