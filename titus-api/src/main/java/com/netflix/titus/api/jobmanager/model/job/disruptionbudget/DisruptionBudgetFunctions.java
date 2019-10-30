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

package com.netflix.titus.api.jobmanager.model.job.disruptionbudget;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;

public final class DisruptionBudgetFunctions {

    private DisruptionBudgetFunctions() {
    }

    private static boolean isLegacyJobDescriptor(JobDescriptor<?> jobDescriptor) {
        DisruptionBudget budget = jobDescriptor.getDisruptionBudget();
        if (budget == null) {
            return true;
        }
        if (budget.getDisruptionBudgetPolicy() instanceof SelfManagedDisruptionBudgetPolicy) {
            SelfManagedDisruptionBudgetPolicy policy = (SelfManagedDisruptionBudgetPolicy) budget.getDisruptionBudgetPolicy();
            return policy.getRelocationTimeMs() == 0;
        }
        return false;
    }

    public static boolean isLegacyJob(Job<?> job) {
        return isLegacyJobDescriptor(job.getJobDescriptor());
    }

    public static boolean isSelfManaged(Job<?> job) {
        return job.getJobDescriptor().getDisruptionBudget().getDisruptionBudgetPolicy() instanceof SelfManagedDisruptionBudgetPolicy;
    }
}
