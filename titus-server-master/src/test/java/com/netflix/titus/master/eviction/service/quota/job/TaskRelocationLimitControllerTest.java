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

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.testkit.model.job.JobComponentStub;
import org.junit.Test;

import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.changeBatchJobSize;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.budget;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.exceptPolicy;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.hourlyRatePercentage;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.newBatchJob;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.perTaskRelocationLimitPolicy;
import static org.assertj.core.api.Assertions.assertThat;

public class TaskRelocationLimitControllerTest {

    private static final Job<BatchJobExt> REFERENCE_JOB = newBatchJob(
            10,
            budget(perTaskRelocationLimitPolicy(10), hourlyRatePercentage(100), Collections.emptyList())
    );

    private final TitusRuntime titusRuntime = TitusRuntimes.test();

    private final JobComponentStub jobComponentStub = new JobComponentStub(titusRuntime);

    private final V3JobOperations jobOperations = jobComponentStub.getJobOperations();

    @Test
    public void testQuota() {
        Job<BatchJobExt> job = createBatchJob(2);
        Task task = jobOperations.getTasks(job.getId()).get(0);

        TaskRelocationLimitController quotaController = new TaskRelocationLimitController(job, jobOperations);
        assertThat(quotaController.getQuota()).isEqualTo(10);

        // Now consume quotas for the first task
        assertThat(quotaController.consume(task.getId())).isTrue();
        assertThat(quotaController.getQuota()).isEqualTo(10);

        assertThat(quotaController.consume(task.getId())).isTrue();
        assertThat(quotaController.getQuota()).isEqualTo(9);

        assertThat(quotaController.consume(task.getId())).isFalse();
    }

    @Test
    public void testJobUpdate() {
        Job<BatchJobExt> job = createBatchJob(1);
        Task task = jobOperations.getTasks(job.getId()).get(0);

        // Consume in first instance of the controller
        TaskRelocationLimitController firstController = new TaskRelocationLimitController(job, jobOperations);
        assertThat(firstController.consume(task.getId())).isTrue();
        assertThat(firstController.consume(task.getId())).isFalse();

        // Update
        Job<BatchJobExt> updatedJob = exceptPolicy(changeBatchJobSize(job, 20), perTaskRelocationLimitPolicy(2));
        jobComponentStub.changeJob(updatedJob);
        jobComponentStub.createDesiredTasks(updatedJob);

        TaskRelocationLimitController updatedController = firstController.update(updatedJob);
        assertThat(updatedController.getQuota()).isEqualTo(20);

        // Consume again, after limit increase
        assertThat(updatedController.consume(task.getId())).isTrue();
        assertThat(updatedController.consume(task.getId())).isFalse();
        assertThat(updatedController.getQuota()).isEqualTo(19);
    }

    private Job<BatchJobExt> createBatchJob(int limit) {
        Job<BatchJobExt> job = exceptPolicy(REFERENCE_JOB, perTaskRelocationLimitPolicy(limit));
        return jobComponentStub.createJobAndTasks(job).getLeft();
    }
}