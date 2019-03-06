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

package com.netflix.titus.master.eviction.service.quota.job;

import com.netflix.titus.api.eviction.model.EvictionQuota;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.PercentagePerHourDisruptionBudgetRate;
import com.netflix.titus.api.model.reference.Reference;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.histogram.RollingCount;
import com.netflix.titus.master.eviction.service.quota.ConsumptionResult;
import com.netflix.titus.master.eviction.service.quota.QuotaController;

public class JobPercentagePerHourRelocationRateController implements QuotaController<Job<?>> {

    private static final long STEP_TIME_MS = 60_000;
    private static final int STEPS = 60;

    private final RollingCount rollingCount;
    private final int limitPerHour;
    private final ConsumptionResult rejectionResult;

    private final EffectiveJobDisruptionBudgetResolver budgetResolver;
    private final TitusRuntime titusRuntime;

    public JobPercentagePerHourRelocationRateController(Job<?> job,
                                                        EffectiveJobDisruptionBudgetResolver budgetResolver,
                                                        TitusRuntime titusRuntime) {
        this.budgetResolver = budgetResolver;
        this.titusRuntime = titusRuntime;

        this.rollingCount = new RollingCount(STEP_TIME_MS, STEPS, titusRuntime.getClock().wallTime());
        this.limitPerHour = computeLimitPerHour(job);
        this.rejectionResult = buildRejectionResult();
    }

    private JobPercentagePerHourRelocationRateController(Job<?> newJob,
                                                         JobPercentagePerHourRelocationRateController previous) {
        this.budgetResolver = previous.budgetResolver;
        this.titusRuntime = previous.titusRuntime;
        this.rollingCount = previous.rollingCount;
        this.limitPerHour = computeLimitPerHour(newJob);
        this.rejectionResult = buildRejectionResult();
    }

    @Override
    public EvictionQuota getQuota(Reference reference) {
        long quota = getQuota(titusRuntime.getClock().wallTime());
        return quota > 0
                ? EvictionQuota.newBuilder().withReference(reference).withQuota(quota).withMessage("Per hour limit %s", limitPerHour).build()
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
    public JobPercentagePerHourRelocationRateController update(Job<?> newJob) {
        return new JobPercentagePerHourRelocationRateController(newJob, this);
    }

    private ConsumptionResult buildRejectionResult() {
        return ConsumptionResult.rejected("Exceeded the number of tasks that can be evicted in an hour (limit=" + limitPerHour + ')');
    }

    private int computeLimitPerHour(Job<?> job) {
        PercentagePerHourDisruptionBudgetRate budgetRate = (PercentagePerHourDisruptionBudgetRate) budgetResolver.resolve(job).getDisruptionBudgetRate();

        double percentage = budgetRate.getMaxPercentageOfContainersRelocatedInHour();
        int desired = JobFunctions.getJobDesiredSize(job);

        int limit = (int) ((desired * percentage) / 100);

        return Math.max(1, limit);
    }

    private int getQuota(long now) {
        int terminatedInLastHour = (int) rollingCount.getCounts(now);
        return limitPerHour - terminatedInLastHour;
    }
}
