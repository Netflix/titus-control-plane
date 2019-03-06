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

import com.google.common.base.Preconditions;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudget;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.SelfManagedDisruptionBudgetPolicy;

/**
 * Resolver that points back to the job's disruption budget.
 */
public class SelfJobDisruptionBudgetResolver implements EffectiveJobDisruptionBudgetResolver {

    private static final EffectiveJobDisruptionBudgetResolver INSTANCE = new SelfJobDisruptionBudgetResolver();

    @Override
    public DisruptionBudget resolve(Job<?> job) {
        Preconditions.checkArgument(
                !(job.getJobDescriptor().getDisruptionBudget().getDisruptionBudgetPolicy() instanceof SelfManagedDisruptionBudgetPolicy),
                "Self managed policy fallback policy not allowed"
        );
        return job.getJobDescriptor().getDisruptionBudget();
    }

    public static EffectiveJobDisruptionBudgetResolver getInstance() {
        return INSTANCE;
    }
}
