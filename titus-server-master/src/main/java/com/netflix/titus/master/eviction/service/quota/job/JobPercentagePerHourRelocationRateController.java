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

import java.util.concurrent.TimeUnit;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.PercentagePerHourDisruptionBudgetRate;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.tuple.Pair;

public class JobPercentagePerHourRelocationRateController extends AbstractRatePerIntervalRateController {

    private final EffectiveJobDisruptionBudgetResolver budgetResolver;

    private JobPercentagePerHourRelocationRateController(Job<?> job,
                                                         long intervalMs,
                                                         long evictionsPerInterval,
                                                         EffectiveJobDisruptionBudgetResolver budgetResolver,
                                                         TitusRuntime titusRuntime) {
        super(job, intervalMs, evictionsPerInterval, titusRuntime);
        this.budgetResolver = budgetResolver;
    }

    private JobPercentagePerHourRelocationRateController(Job<?> newJob,
                                                         long intervalMs,
                                                         long evictionsPerInterval,
                                                         JobPercentagePerHourRelocationRateController previous) {
        super(newJob, intervalMs, evictionsPerInterval, previous);
        this.budgetResolver = previous.budgetResolver;
    }

    @Override
    public JobPercentagePerHourRelocationRateController update(Job<?> newJob) {
        Pair<Long, Integer> pair = getIntervalLimitPair(newJob, budgetResolver);
        return new JobPercentagePerHourRelocationRateController(newJob, pair.getLeft(), pair.getRight(), this);
    }

    public static JobPercentagePerHourRelocationRateController newJobPercentagePerHourRelocationRateController(Job<?> job,
                                                                                                               EffectiveJobDisruptionBudgetResolver budgetResolver,
                                                                                                               TitusRuntime titusRuntime) {
        Pair<Long, Integer> pair = getIntervalLimitPair(job, budgetResolver);
        return new JobPercentagePerHourRelocationRateController(job, pair.getLeft(), pair.getRight(), budgetResolver, titusRuntime);
    }

    private static Pair<Long, Integer> getIntervalLimitPair(Job<?> job, EffectiveJobDisruptionBudgetResolver budgetResolver) {
        PercentagePerHourDisruptionBudgetRate rate = (PercentagePerHourDisruptionBudgetRate) budgetResolver.resolve(job).getDisruptionBudgetRate();
        long intervalMs = TimeUnit.HOURS.toMillis(1);

        double percentage = rate.getMaxPercentageOfContainersRelocatedInHour();
        int desired = JobFunctions.getJobDesiredSize(job);
        int evictionsPerInterval = (int) ((desired * percentage) / 100);

        return Pair.of(intervalMs, Math.max(1, evictionsPerInterval));
    }
}
