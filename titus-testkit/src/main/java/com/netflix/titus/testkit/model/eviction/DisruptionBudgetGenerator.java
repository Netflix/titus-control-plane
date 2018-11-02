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

package com.netflix.titus.testkit.model.eviction;

import java.util.Collections;
import java.util.List;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor.JobDescriptorExt;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.AvailabilityPercentageLimitDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.Day;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudget;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudgetRate;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.PercentagePerHourDisruptionBudgetRate;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.RelocationLimitDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.SelfManagedDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.TimeWindow;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.UnhealthyTasksLimitDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.UnlimitedDisruptionBudgetRate;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import com.netflix.titus.testkit.model.job.JobGenerator;

public final class DisruptionBudgetGenerator {

    public static AvailabilityPercentageLimitDisruptionBudgetPolicy percentageOfHealthyPolicy(double percentage) {
        return AvailabilityPercentageLimitDisruptionBudgetPolicy.newBuilder()
                .withPercentageOfHealthyContainers(percentage)
                .build();
    }

    public static UnhealthyTasksLimitDisruptionBudgetPolicy numberOfHealthyPolicy(int limit) {
        return UnhealthyTasksLimitDisruptionBudgetPolicy.newBuilder()
                .withLimitOfUnhealthyContainers(limit)
                .build();
    }

    public static RelocationLimitDisruptionBudgetPolicy perTaskRelocationLimitPolicy(int limit) {
        return RelocationLimitDisruptionBudgetPolicy.newBuilder()
                .withLimit(limit)
                .build();
    }

    public static SelfManagedDisruptionBudgetPolicy selfManagedPolicy(long relocationTimeMs) {
        return SelfManagedDisruptionBudgetPolicy.newBuilder()
                .withRelocationTimeMs(relocationTimeMs)
                .build();
    }

    public static PercentagePerHourDisruptionBudgetRate hourlyRatePercentage(int rate) {
        return PercentagePerHourDisruptionBudgetRate.newBuilder()
                .withMaxPercentageOfContainersRelocatedInHour(rate)
                .build();
    }

    public static UnlimitedDisruptionBudgetRate unlimitedRate() {
        return UnlimitedDisruptionBudgetRate.newBuilder().build();
    }

    public static TimeWindow officeHourTimeWindow() {
        return TimeWindow.newBuilder()
                .withDays(Day.weekdays())
                .withwithHourlyTimeWindows(8, 17)
                .build();
    }

    public static DisruptionBudget budget(DisruptionBudgetPolicy policy,
                                          DisruptionBudgetRate rate,
                                          List<TimeWindow> timeWindows) {
        return DisruptionBudget.newBuilder()
                .withDisruptionBudgetPolicy(policy)
                .withDisruptionBudgetRate(rate)
                .withContainerHealthProviders(Collections.emptyList())
                .withTimeWindows(timeWindows)
                .build();
    }

    public static JobDescriptor<BatchJobExt> newBatchJobDescriptor(int desired, DisruptionBudget budget) {
        return JobDescriptorGenerator.batchJobDescriptor(desired).toBuilder()
                .withDisruptionBudget(budget)
                .build();
    }

    public static Job<BatchJobExt> newBatchJob(int desired, DisruptionBudget budget) {
        return JobGenerator.batchJobs(newBatchJobDescriptor(desired, budget)).getValue();
    }

    public static <E extends JobDescriptorExt> JobDescriptor<E> exceptBudget(JobDescriptor<E> jobDescriptor, DisruptionBudget budget) {
        return jobDescriptor.toBuilder().withDisruptionBudget(budget).build();
    }

    public static <E extends JobDescriptorExt> Job<E> exceptBudget(Job<E> job, DisruptionBudget budget) {
        return job.toBuilder().withJobDescriptor(exceptBudget(job.getJobDescriptor(), budget)).build();
    }

    public static <E extends JobDescriptorExt> JobDescriptor<E> exceptPolicy(JobDescriptor<E> jobDescriptor, DisruptionBudgetPolicy policy) {
        return exceptBudget(jobDescriptor, jobDescriptor.getDisruptionBudget().toBuilder().withDisruptionBudgetPolicy(policy).build());
    }

    public static <E extends JobDescriptorExt> Job<E> exceptPolicy(Job<E> job, DisruptionBudgetPolicy policy) {
        return exceptBudget(job, job.getJobDescriptor().getDisruptionBudget().toBuilder().withDisruptionBudgetPolicy(policy).build());
    }

    public static <E extends JobDescriptorExt> JobDescriptor<E> exceptRate(JobDescriptor<E> jobDescriptor, DisruptionBudgetRate rate) {
        return exceptBudget(jobDescriptor, jobDescriptor.getDisruptionBudget().toBuilder().withDisruptionBudgetRate(rate).build());
    }

    public static <E extends JobDescriptorExt> Job<E> exceptRate(Job<E> job, DisruptionBudgetRate rate) {
        return exceptBudget(job, job.getJobDescriptor().getDisruptionBudget().toBuilder().withDisruptionBudgetRate(rate).build());
    }
}
