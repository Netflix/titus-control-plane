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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.api.containerhealth.service.ContainerHealthService;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.AvailabilityPercentageLimitDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudget;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudgetFunctions;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.PercentagePerHourDisruptionBudgetRate;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.RelocationLimitDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.SelfManagedDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.UnhealthyTasksLimitDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.master.eviction.service.quota.ConsumptionResult;
import com.netflix.titus.master.eviction.service.quota.QuotaController;
import com.netflix.titus.master.eviction.service.quota.QuotaTracker;
import com.netflix.titus.master.eviction.service.quota.TimeWindowQuotaTracker;

public class JobQuotaController implements QuotaController<Job<?>> {

    private static final ConsumptionResult LEGACY = ConsumptionResult.rejected("Legacy job");

    private final Job<?> job;
    private final V3JobOperations jobOperations;
    private final ContainerHealthService containerHealthService;
    private final TitusRuntime titusRuntime;
    private final List<QuotaTracker> quotaTrackers;
    private final List<QuotaController<Job<?>>> quotaControllers;

    public JobQuotaController(Job<?> job,
                              V3JobOperations jobOperations,
                              ContainerHealthService containerHealthService,
                              TitusRuntime titusRuntime) {
        this.job = job;
        this.jobOperations = jobOperations;
        this.containerHealthService = containerHealthService;
        this.titusRuntime = titusRuntime;

        if (DisruptionBudgetFunctions.isLegacyJob(job)) {
            this.quotaTrackers = Collections.emptyList();
            this.quotaControllers = Collections.emptyList();
        } else {
            this.quotaTrackers = buildQuotaTrackers(job, jobOperations, containerHealthService, titusRuntime);
            this.quotaControllers = buildQuotaControllers(job, jobOperations, titusRuntime);
        }
    }

    private JobQuotaController(Job<?> newJob,
                               V3JobOperations jobOperations,
                               ContainerHealthService containerHealthService,
                               JobQuotaController previousJobQuotaController,
                               TitusRuntime titusRuntime) {
        this.job = newJob;
        this.jobOperations = jobOperations;
        this.containerHealthService = containerHealthService;
        this.titusRuntime = titusRuntime;

        if (DisruptionBudgetFunctions.isLegacyJob(newJob)) {
            this.quotaTrackers = Collections.emptyList();
            this.quotaControllers = Collections.emptyList();
        } else {
            this.quotaTrackers = buildQuotaTrackers(job, jobOperations, containerHealthService, titusRuntime);
            this.quotaControllers = mergeQuotaControllers(previousJobQuotaController.quotaControllers, newJob, jobOperations, titusRuntime);
        }
    }

    public Job<?> getJob() {
        return job;
    }

    @Override
    public long getQuota() {
        if (isLegacy()) {
            return 0;
        }
        return getMinSubQuota();
    }

    @Override
    public Optional<String> explainRestrictions(String taskId) {
        return Optional.empty();
    }

    @Override
    public ConsumptionResult consume(String taskId) {
        if (isLegacy()) {
            return LEGACY;
        }

        StringBuilder rejectionResponseBuilder = new StringBuilder("MissingQuotas[");

        // Check quota trackers first
        boolean noQuota = false;
        for (QuotaTracker tracker : quotaTrackers) {
            if (tracker.getQuota() <= 0) {
                noQuota = true;
                String restrictions = tracker.explainRestrictions(taskId).orElse("no quota");
                rejectionResponseBuilder.append(tracker.getClass().getSimpleName()).append('=').append(restrictions).append(", ");
            }
        }
        if (noQuota) {
            rejectionResponseBuilder.setLength(rejectionResponseBuilder.length() - 2);
            return ConsumptionResult.rejected(rejectionResponseBuilder.append(']').toString());
        }

        // Now controllers
        for (int i = 0; i < quotaControllers.size(); i++) {
            QuotaController<Job<?>> controller = quotaControllers.get(i);
            ConsumptionResult result = controller.consume(taskId);
            if (!result.isApproved()) {
                for (int j = 0; j < i; j++) {
                    quotaControllers.get(j).giveBackConsumedQuota(taskId);
                }
                rejectionResponseBuilder
                        .append(controller.getClass().getSimpleName())
                        .append('=')
                        .append(result.getRejectionReason().orElse("no quota"));
                return ConsumptionResult.rejected(rejectionResponseBuilder.append(']').toString());
            }
        }

        return ConsumptionResult.approved();
    }

    @Override
    public void giveBackConsumedQuota(String taskId) {
        quotaControllers.forEach(c -> c.giveBackConsumedQuota(taskId));
    }

    @Override
    public JobQuotaController update(Job<?> updatedJob) {
        if (DisruptionBudgetFunctions.isLegacyJob(updatedJob) && isLegacy()) {
            return this;
        }

        int currentDesired = JobFunctions.getJobDesiredSize(job);
        int newDesired = JobFunctions.getJobDesiredSize(updatedJob);

        if (currentDesired == newDesired) {
            if (job.getJobDescriptor().getDisruptionBudget().equals(updatedJob.getJobDescriptor().getDisruptionBudget())) {
                return this;
            }
        }

        return new JobQuotaController(
                updatedJob,
                jobOperations,
                containerHealthService,
                this,
                titusRuntime
        );
    }

    private boolean isLegacy() {
        return quotaTrackers.isEmpty() && quotaControllers.isEmpty();
    }

    private long getMinSubQuota() {
        return Math.min(getMinSubQuota(quotaTrackers), getMinSubQuota(quotaControllers));
    }

    private long getMinSubQuota(List<? extends QuotaTracker> quotaTrackers) {
        long result = Long.MAX_VALUE / 2;
        for (QuotaTracker quotaTracker : quotaTrackers) {
            result = Math.min(result, quotaTracker.getQuota());
        }
        return result;
    }

    @VisibleForTesting
    static List<QuotaTracker> buildQuotaTrackers(Job<?> job,
                                                 V3JobOperations jobOperations,
                                                 ContainerHealthService containerHealthService,
                                                 TitusRuntime titusRuntime) {
        List<QuotaTracker> quotaTrackers = new ArrayList<>();

        DisruptionBudget budget = job.getJobDescriptor().getDisruptionBudget();

        if (!budget.getTimeWindows().isEmpty()) {
            quotaTrackers.add(new TimeWindowQuotaTracker(budget.getTimeWindows(), titusRuntime));
        }

        DisruptionBudgetPolicy policy = budget.getDisruptionBudgetPolicy();
        if (policy instanceof AvailabilityPercentageLimitDisruptionBudgetPolicy) {
            quotaTrackers.add(UnhealthyTasksLimitTracker.percentageLimit(job, jobOperations, containerHealthService));
        } else if (policy instanceof UnhealthyTasksLimitDisruptionBudgetPolicy) {
            quotaTrackers.add(UnhealthyTasksLimitTracker.absoluteLimit(job, jobOperations, containerHealthService));
        } else if (policy instanceof SelfManagedDisruptionBudgetPolicy) {
            quotaTrackers.add(SelfManagedPolicyTracker.getInstance());
        }

        return quotaTrackers;
    }

    @VisibleForTesting
    static List<QuotaController<Job<?>>> buildQuotaControllers(Job<?> job,
                                                               V3JobOperations jobOperations,
                                                               TitusRuntime titusRuntime) {
        List<QuotaController<Job<?>>> quotaControllers = new ArrayList<>();

        DisruptionBudget budget = job.getJobDescriptor().getDisruptionBudget();

        if (budget.getDisruptionBudgetRate() instanceof PercentagePerHourDisruptionBudgetRate) {
            quotaControllers.add(new JobPercentagePerHourRelocationRateController(job, titusRuntime));
        }

        DisruptionBudgetPolicy policy = budget.getDisruptionBudgetPolicy();
        if (policy instanceof RelocationLimitDisruptionBudgetPolicy) {
            quotaControllers.add(new TaskRelocationLimitController(job, jobOperations));
        }
        return quotaControllers;
    }

    @VisibleForTesting
    static List<QuotaController<Job<?>>> mergeQuotaControllers(List<QuotaController<Job<?>>> previousControllers,
                                                               Job<?> job,
                                                               V3JobOperations jobOperations,
                                                               TitusRuntime titusRuntime) {
        List<QuotaController<Job<?>>> quotaControllers = new ArrayList<>();

        DisruptionBudget budget = job.getJobDescriptor().getDisruptionBudget();

        if (budget.getDisruptionBudgetRate() instanceof PercentagePerHourDisruptionBudgetRate) {
            QuotaController<Job<?>> newController = mergeQuotaController(job,
                    previousControllers,
                    JobPercentagePerHourRelocationRateController.class,
                    () -> new JobPercentagePerHourRelocationRateController(job, titusRuntime)
            );
            quotaControllers.add(newController);
        }

        DisruptionBudgetPolicy policy = budget.getDisruptionBudgetPolicy();
        if (policy instanceof RelocationLimitDisruptionBudgetPolicy) {
            QuotaController<Job<?>> newController = mergeQuotaController(job,
                    previousControllers,
                    TaskRelocationLimitController.class,
                    () -> new TaskRelocationLimitController(job, jobOperations)
            );
            quotaControllers.add(newController);
        }
        return quotaControllers;
    }

    private static QuotaController<Job<?>> mergeQuotaController(Job<?> job,
                                                                List<QuotaController<Job<?>>> previousQuotaControllers,
                                                                Class<? extends QuotaController<Job<?>>> type,
                                                                Supplier<QuotaController<Job<?>>> quotaControllerFactory) {
        for (QuotaController<Job<?>> next : previousQuotaControllers) {
            if (type.isAssignableFrom(next.getClass())) {
                return next.update(job);
            }
        }
        return quotaControllerFactory.get();
    }
}
