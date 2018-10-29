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

import java.util.List;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.api.containerhealth.model.ContainerHealthState;
import com.netflix.titus.api.containerhealth.model.ContainerHealthStatus;
import com.netflix.titus.api.containerhealth.service.ContainerHealthService;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.AvailabilityPercentageLimitDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.UnhealthyTasksLimitDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.master.eviction.service.quota.QuotaTracker;

public class UnhealthyTasksLimitTracker implements QuotaTracker {

    private final Job<?> job;
    private final int minimumHealthyCount;

    private final V3JobOperations jobOperations;
    private final ContainerHealthService containerHealthService;

    private UnhealthyTasksLimitTracker(Job<?> job,
                                      int minimumHealthyCount,
                                      V3JobOperations jobOperations,
                                      ContainerHealthService containerHealthService) {
        this.job = job;
        this.minimumHealthyCount = minimumHealthyCount;
        this.jobOperations = jobOperations;
        this.containerHealthService = containerHealthService;
    }

    @Override
    public long getQuota() {
        List<Task> tasks;
        try {
            tasks = jobOperations.getTasks(job.getId());
        } catch (JobManagerException e) {
            return 0;
        }

        // Check first how many started tasks we have
        int started = 0;
        for (Task task : tasks) {
            if (task.getStatus().getState() == TaskState.Started) {
                started++;
            }
        }
        if (started < minimumHealthyCount) {
            return 0;
        }

        // We have enough tasks started. Check now how many are healthy.
        int healthy = 0;
        for (Task task : tasks) {
            if (task.getStatus().getState() == TaskState.Started) {
                Optional<ContainerHealthStatus> status = containerHealthService.findHealthStatus(task.getId());
                if (status.isPresent() && status.get().getState() == ContainerHealthState.Healthy) {
                    healthy++;
                }
            }
        }
        return Math.max(0, healthy - minimumHealthyCount);
    }

    public static UnhealthyTasksLimitTracker percentageLimit(Job<?> job,
                                                             V3JobOperations jobOperations,
                                                             ContainerHealthService containerHealthService) {

        return new UnhealthyTasksLimitTracker(job, computeHealthyPoolSizeFromPercentage(job), jobOperations, containerHealthService);
    }

    public static UnhealthyTasksLimitTracker absoluteLimit(Job<?> job,
                                                           V3JobOperations jobOperations,
                                                           ContainerHealthService containerHealthService) {
        return new UnhealthyTasksLimitTracker(job, computeHealthyPoolSizeFromAbsoluteLimit(job), jobOperations, containerHealthService);
    }

    @VisibleForTesting
    static int computeHealthyPoolSizeFromAbsoluteLimit(Job<?> job) {
        UnhealthyTasksLimitDisruptionBudgetPolicy policy = (UnhealthyTasksLimitDisruptionBudgetPolicy)
                job.getJobDescriptor().getDisruptionBudget().getDisruptionBudgetPolicy();
        return Math.max(0, JobFunctions.getJobDesiredSize(job) - Math.max(1, policy.getLimitOfUnhealthyContainers()));
    }

    @VisibleForTesting
    static int computeHealthyPoolSizeFromPercentage(Job<?> job) {
        int jobDesiredSize = JobFunctions.getJobDesiredSize(job);
        if (jobDesiredSize == 0) {
            return 0;
        }

        AvailabilityPercentageLimitDisruptionBudgetPolicy policy = (AvailabilityPercentageLimitDisruptionBudgetPolicy)
                job.getJobDescriptor().getDisruptionBudget().getDisruptionBudgetPolicy();

        double percentageOfHealthy = policy.getPercentageOfHealthyContainers();

        int minimumHealthyCount = (int) Math.ceil((percentageOfHealthy * jobDesiredSize) / 100);
        return Math.min(minimumHealthyCount, jobDesiredSize - 1);
    }
}
