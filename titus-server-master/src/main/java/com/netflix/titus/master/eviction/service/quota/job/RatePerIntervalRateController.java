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
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudget;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.RatePerIntervalDisruptionBudgetRate;
import com.netflix.titus.api.model.reference.Reference;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.histogram.RollingCount;
import com.netflix.titus.master.eviction.service.quota.ConsumptionResult;
import com.netflix.titus.master.eviction.service.quota.QuotaController;

public class RatePerIntervalRateController implements QuotaController<Job<?>> {

    private static final int RESOLUTION = 20;

    private final RollingCount rollingCount;
    private final RatePerIntervalDisruptionBudgetRate rate;
    private final ConsumptionResult rejectionResult;

    private final EffectiveJobDisruptionBudgetResolver budgetResolver;
    private final TitusRuntime titusRuntime;

    public RatePerIntervalRateController(Job<?> job,
                                         EffectiveJobDisruptionBudgetResolver budgetResolver,
                                         TitusRuntime titusRuntime) {
        this.budgetResolver = budgetResolver;
        this.titusRuntime = titusRuntime;

        this.rate = getRate(job);
        this.rollingCount = RollingCount.rollingWindow(rate.getIntervalMs(), RESOLUTION, titusRuntime.getClock().wallTime());
        this.rejectionResult = buildRejectionResult(rate);
    }

    private RatePerIntervalRateController(Job<?> newJob,
                                          RatePerIntervalRateController previous) {
        this.budgetResolver = previous.budgetResolver;
        this.titusRuntime = previous.titusRuntime;

        this.rate = getRate(newJob);

        if (rate.getIntervalMs() == previous.rate.getIntervalMs()) {
            this.rollingCount = previous.rollingCount;
        } else {
            long now = titusRuntime.getClock().wallTime();

            this.rollingCount = RollingCount.rollingWindow(rate.getIntervalMs(), RESOLUTION, titusRuntime.getClock().wallTime());
            rollingCount.add(Math.min(rate.getLimitPerInterval(), previous.rollingCount.getCounts(now)), now);
        }
        this.rejectionResult = buildRejectionResult(rate);
    }

    @Override
    public EvictionQuota getQuota(Reference reference) {
        long quota = getQuota(titusRuntime.getClock().wallTime());
        return quota > 0
                ? EvictionQuota.newBuilder().withReference(reference).withQuota(quota).withMessage("Per interval limit %s", rate.getLimitPerInterval()).build()
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

    @Override
    public RatePerIntervalRateController update(Job<?> newJob) {
        return new RatePerIntervalRateController(newJob, this);
    }

    private RatePerIntervalDisruptionBudgetRate getRate(Job<?> job) {
        DisruptionBudget budget = budgetResolver.resolve(job);
        return (RatePerIntervalDisruptionBudgetRate) budget.getDisruptionBudgetRate();
    }

    private ConsumptionResult buildRejectionResult(RatePerIntervalDisruptionBudgetRate rate) {
        return ConsumptionResult.rejected(String.format("Exceeded the number of tasks that can be evicted in %sms (limit=%s)", rate.getIntervalMs(), rate.getLimitPerInterval()));
    }

    private int getQuota(long now) {
        int terminatedInLastHour = (int) rollingCount.getCounts(now);
        return rate.getLimitPerInterval() - terminatedInLastHour;
    }
}
