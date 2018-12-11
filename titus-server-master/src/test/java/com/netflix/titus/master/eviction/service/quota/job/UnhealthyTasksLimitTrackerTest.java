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

import java.util.Collections;

import com.netflix.titus.api.containerhealth.model.ContainerHealthState;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.model.reference.Reference;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.testkit.model.job.JobComponentStub;
import org.junit.Test;

import static com.netflix.titus.master.eviction.service.quota.job.UnhealthyTasksLimitTracker.computeHealthyPoolSizeFromAbsoluteLimit;
import static com.netflix.titus.master.eviction.service.quota.job.UnhealthyTasksLimitTracker.computeHealthyPoolSizeFromPercentage;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.budget;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.newBatchJob;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.numberOfHealthyPolicy;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.percentageOfHealthyPolicy;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.unlimitedRate;
import static org.assertj.core.api.Assertions.assertThat;

public class UnhealthyTasksLimitTrackerTest {

    private final TitusRuntime titusRuntime = TitusRuntimes.test();

    private final JobComponentStub jobComponentStub = new JobComponentStub(titusRuntime);

    private final V3JobOperations jobOperations = jobComponentStub.getJobOperations();

    @Test
    public void testComputeHealthyPoolSizeFromPercentage() {
        assertThat(computeHealthyPoolSizeFromPercentage(newBatchJobWithPercentageLimit(0, 80))).isEqualTo(0);
        assertThat(computeHealthyPoolSizeFromPercentage(newBatchJobWithPercentageLimit(1, 80))).isEqualTo(0);
        assertThat(computeHealthyPoolSizeFromPercentage(newBatchJobWithPercentageLimit(2, 80))).isEqualTo(1);
        assertThat(computeHealthyPoolSizeFromPercentage(newBatchJobWithPercentageLimit(10, 80))).isEqualTo(8);
    }

    @Test
    public void testComputeHealthyPoolSizeFromAbsoluteLimit() {
        assertThat(computeHealthyPoolSizeFromAbsoluteLimit(newBatchJobWithAbsoluteLimit(0, 10))).isEqualTo(0);
        assertThat(computeHealthyPoolSizeFromAbsoluteLimit(newBatchJobWithAbsoluteLimit(1, 0))).isEqualTo(0);
        assertThat(computeHealthyPoolSizeFromAbsoluteLimit(newBatchJobWithAbsoluteLimit(1, 2))).isEqualTo(0);
        assertThat(computeHealthyPoolSizeFromAbsoluteLimit(newBatchJobWithAbsoluteLimit(10, 5))).isEqualTo(5);
    }

    @Test
    public void testPercentageBasedQuota() {
        Job<BatchJobExt> job = newBatchJobWithPercentageLimit(10, 80);
        UnhealthyTasksLimitTracker tracker = UnhealthyTasksLimitTracker.percentageLimit(
                job,
                jobOperations,
                jobComponentStub.getContainerHealthService()
        );
        testQuota(tracker, Reference.job(job.getId()));
    }

    @Test
    public void testAbsoluteLimitBasedQuota() {
        Job<?> job = newBatchJobWithAbsoluteLimit(10, 2);
        UnhealthyTasksLimitTracker tracker = UnhealthyTasksLimitTracker.absoluteLimit(
                job,
                jobOperations,
                jobComponentStub.getContainerHealthService()
        );
        testQuota(tracker, Reference.job(job.getId()));
    }

    private void testQuota(UnhealthyTasksLimitTracker tracker, Reference jobReference) {
        // No tasks are started yet
        assertThat(tracker.getQuota(jobReference).getQuota()).isEqualTo(0);

        // Start all of them
        jobOperations.getTasks().forEach(task -> jobComponentStub.moveTaskToState(task, TaskState.Started));
        assertThat(tracker.getQuota(jobReference).getQuota()).isEqualTo(2);

        // Now make one unhealthy
        jobComponentStub.changeContainerHealth(jobOperations.getTasks().get(0).getId(), ContainerHealthState.Unhealthy);
        assertThat(tracker.getQuota(jobReference).getQuota()).isEqualTo(1);
    }

    private Job<BatchJobExt> newBatchJobWithPercentageLimit(int desired, int percentage) {
        Job<BatchJobExt> job = newBatchJob(desired, budget(percentageOfHealthyPolicy(percentage), unlimitedRate(), Collections.emptyList()));
        jobComponentStub.createJobAndTasks(job);
        return job;
    }

    private Job<?> newBatchJobWithAbsoluteLimit(int desired, int absoluteLimit) {
        Job<BatchJobExt> job = newBatchJob(desired, budget(numberOfHealthyPolicy(absoluteLimit), unlimitedRate(), Collections.emptyList()));
        jobComponentStub.createJobAndTasks(job);
        return job;
    }
}