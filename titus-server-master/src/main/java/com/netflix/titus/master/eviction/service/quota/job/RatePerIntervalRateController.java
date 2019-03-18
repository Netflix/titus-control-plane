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
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.RatePerIntervalDisruptionBudgetRate;
import com.netflix.titus.common.runtime.TitusRuntime;

public class RatePerIntervalRateController extends AbstractRatePerIntervalRateController {

    private final EffectiveJobDisruptionBudgetResolver budgetResolver;

    private RatePerIntervalRateController(Job<?> job,
                                          long intervalMs,
                                          long evictionsPerInterval,
                                          EffectiveJobDisruptionBudgetResolver budgetResolver,
                                          TitusRuntime titusRuntime) {
        super(job, intervalMs, evictionsPerInterval, titusRuntime);
        this.budgetResolver = budgetResolver;
    }

    private RatePerIntervalRateController(Job<?> newJob,
                                          long intervalMs,
                                          long evictionsPerInterval,
                                          RatePerIntervalRateController previous) {
        super(newJob, intervalMs, evictionsPerInterval, previous);
        this.budgetResolver = previous.budgetResolver;
    }

    @Override
    public RatePerIntervalRateController update(Job<?> newJob) {
        RatePerIntervalDisruptionBudgetRate rate = (RatePerIntervalDisruptionBudgetRate) budgetResolver.resolve(newJob).getDisruptionBudgetRate();
        return new RatePerIntervalRateController(
                newJob,
                rate.getIntervalMs(),
                rate.getLimitPerInterval(),
                this
        );
    }

    public static RatePerIntervalRateController newRatePerIntervalRateController(Job<?> job,
                                                                                 EffectiveJobDisruptionBudgetResolver budgetResolver,
                                                                                 TitusRuntime titusRuntime) {
        RatePerIntervalDisruptionBudgetRate rate = (RatePerIntervalDisruptionBudgetRate) budgetResolver.resolve(job).getDisruptionBudgetRate();
        return new RatePerIntervalRateController(job, rate.getIntervalMs(), rate.getLimitPerInterval(), budgetResolver, titusRuntime);
    }
}
