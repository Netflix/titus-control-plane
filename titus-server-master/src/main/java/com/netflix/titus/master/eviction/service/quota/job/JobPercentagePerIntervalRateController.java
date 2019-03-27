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

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.RatePercentagePerIntervalDisruptionBudgetRate;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.tuple.Pair;

public class JobPercentagePerIntervalRateController extends AbstractRatePerIntervalRateController {

    private final EffectiveJobDisruptionBudgetResolver budgetResolver;

    protected JobPercentagePerIntervalRateController(Job<?> job,
                                                     long intervalMs,
                                                     long evictionsPerInterval,
                                                     EffectiveJobDisruptionBudgetResolver budgetResolver,
                                                     TitusRuntime titusRuntime) {
        super(job, intervalMs, evictionsPerInterval, titusRuntime);
        this.budgetResolver = budgetResolver;
    }

    private JobPercentagePerIntervalRateController(Job<?> newJob,
                                                   long intervalMs,
                                                   long evictionsPerInterval,
                                                   JobPercentagePerIntervalRateController previous) {
        super(newJob, intervalMs, evictionsPerInterval, previous);
        this.budgetResolver = previous.budgetResolver;
    }

    @Override
    public JobPercentagePerIntervalRateController update(Job<?> newJob) {
        Pair<Long, Integer> pair = getIntervalLimitPair(newJob, budgetResolver);
        return new JobPercentagePerIntervalRateController(newJob, pair.getLeft(), pair.getRight(), this);
    }

    public static JobPercentagePerIntervalRateController newJobPercentagePerIntervalRateController(Job<?> job,
                                                                                                   EffectiveJobDisruptionBudgetResolver budgetResolver,
                                                                                                   TitusRuntime titusRuntime) {
        Pair<Long, Integer> pair = getIntervalLimitPair(job, budgetResolver);
        return new JobPercentagePerIntervalRateController(job, pair.getLeft(), pair.getRight(), budgetResolver, titusRuntime);
    }

    private static Pair<Long, Integer> getIntervalLimitPair(Job<?> job, EffectiveJobDisruptionBudgetResolver budgetResolver) {
        RatePercentagePerIntervalDisruptionBudgetRate rate = (RatePercentagePerIntervalDisruptionBudgetRate) budgetResolver.resolve(job).getDisruptionBudgetRate();

        long intervalMs = rate.getIntervalMs();

        double percentage = rate.getPercentageLimitPerInterval();
        int desired = JobFunctions.getJobDesiredSize(job);
        int evictionsPerInterval = (int) ((desired * percentage) / 100);

        return Pair.of(intervalMs, Math.max(1, evictionsPerInterval));
    }
}
