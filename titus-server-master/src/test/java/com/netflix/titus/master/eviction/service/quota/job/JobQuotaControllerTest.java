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

import java.time.DayOfWeek;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.netflix.titus.api.containerhealth.service.ContainerHealthService;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudget;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.time.Clocks;
import com.netflix.titus.common.util.time.TestClock;
import com.netflix.titus.master.eviction.service.quota.ConsumptionResult;
import com.netflix.titus.master.eviction.service.quota.QuotaController;
import com.netflix.titus.master.eviction.service.quota.QuotaTracker;
import com.netflix.titus.master.eviction.service.quota.TimeWindowQuotaTracker;
import com.netflix.titus.testkit.model.job.JobComponentStub;
import org.junit.Test;

import static com.netflix.titus.master.eviction.service.quota.job.JobQuotaController.buildQuotaControllers;
import static com.netflix.titus.master.eviction.service.quota.job.JobQuotaController.buildQuotaTrackers;
import static com.netflix.titus.master.eviction.service.quota.job.JobQuotaController.mergeQuotaControllers;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.budget;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.exceptBudget;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.exceptPolicy;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.exceptRate;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.hourlyRatePercentage;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.newBatchJob;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.numberOfHealthyPolicy;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.officeHourTimeWindow;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.perTaskRelocationLimitPolicy;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.percentageOfHealthyPolicy;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.selfManagedPolicy;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.unlimitedRate;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

public class JobQuotaControllerTest {

    private final TestClock clock = Clocks.testWorldClock().jumpForwardTo(DayOfWeek.MONDAY).resetTime(9, 0, 0);

    private final TitusRuntime titusRuntime = TitusRuntimes.test(clock);

    private final JobComponentStub jobComponentStub = new JobComponentStub(titusRuntime);

    private final V3JobOperations jobOperations = jobComponentStub.getJobOperations();

    private final ContainerHealthService containerHealthService = jobComponentStub.getContainerHealthService();

    @Test
    public void testBuildQuotaTrackers() {
        testBuildQuotaTrackers(
                scheduleJob(
                        newBatchJob(10, budget(percentageOfHealthyPolicy(80.0), hourlyRatePercentage(5), Collections.emptyList())),
                        10
                ),
                UnhealthyTasksLimitTracker.class
        );

        testBuildQuotaTrackers(
                scheduleJob(
                        newBatchJob(10, budget(percentageOfHealthyPolicy(80.0), hourlyRatePercentage(5), singletonList(officeHourTimeWindow()))),
                        10
                ),
                UnhealthyTasksLimitTracker.class, TimeWindowQuotaTracker.class
        );

        testBuildQuotaTrackers(
                scheduleJob(
                        newBatchJob(10, budget(numberOfHealthyPolicy(2), hourlyRatePercentage(5), singletonList(officeHourTimeWindow()))),
                        10
                ),
                UnhealthyTasksLimitTracker.class, TimeWindowQuotaTracker.class
        );

        testBuildQuotaTrackers(
                scheduleJob(
                        newBatchJob(10, budget(selfManagedPolicy(10_000), hourlyRatePercentage(5), singletonList(officeHourTimeWindow()))),
                        10
                ),
                SelfManagedPolicyTracker.class, TimeWindowQuotaTracker.class
        );
    }

    private void testBuildQuotaTrackers(Job<?> job, Class<?>... expectedTypes) {
        List<QuotaTracker> trackers = buildQuotaTrackers(job, jobOperations, containerHealthService, titusRuntime);
        checkContains(trackers, expectedTypes);
    }

    @Test
    public void testBuildQuotaControllers() {
        testBuildQuotaControllers(
                scheduleJob(
                        newBatchJob(10, budget(perTaskRelocationLimitPolicy(3), unlimitedRate(), Collections.emptyList())),
                        10
                ),
                TaskRelocationLimitController.class
        );

        testBuildQuotaControllers(
                scheduleJob(
                        newBatchJob(10, budget(perTaskRelocationLimitPolicy(3), hourlyRatePercentage(5), Collections.emptyList())),
                        10
                ),
                JobPercentagePerHourRelocationRateController.class, TaskRelocationLimitController.class
        );
    }

    private void testBuildQuotaControllers(Job<?> job, Class<?>... expectedTypes) {
        List<QuotaController<Job<?>>> controllers = buildQuotaControllers(job, jobOperations, titusRuntime);
        checkContains(controllers, expectedTypes);
    }

    @Test
    public void testMergePercentagePerHourDisruptionBudgetRateQuotaController() {
        // First version
        Job<BatchJobExt> job = newBatchJob(10, budget(perTaskRelocationLimitPolicy(3), hourlyRatePercentage(50), Collections.emptyList()));
        com.netflix.titus.api.model.reference.Reference jobReference = com.netflix.titus.api.model.reference.Reference.job(job.getId());

        scheduleJob(job, 10);

        List<QuotaController<Job<?>>> controllers = buildQuotaControllers(job, jobOperations, titusRuntime);
        JobPercentagePerHourRelocationRateController controller = (JobPercentagePerHourRelocationRateController) controllers.get(0);

        Task task = jobOperations.getTasks(job.getId()).get(0);
        assertThat(controller.consume(task.getId()).isApproved()).isTrue();
        assertThat(controller.getQuota(jobReference).getQuota()).isEqualTo(4);

        // Change job descriptor and consume some quota
        Job<BatchJobExt> updatedJob = jobComponentStub.changeJob(exceptRate(job, hourlyRatePercentage(80)));
        List<QuotaController<Job<?>>> merged = mergeQuotaControllers(controllers, updatedJob, jobOperations, titusRuntime);
        JobPercentagePerHourRelocationRateController updatedController = (JobPercentagePerHourRelocationRateController) merged.get(0);

        assertThat(updatedController.getQuota(jobReference).getQuota()).isEqualTo(7);
    }

    @Test
    public void testMergeTaskRelocationLimitController() {
        // First version
        Job<BatchJobExt> job = newBatchJob(10, budget(perTaskRelocationLimitPolicy(1), unlimitedRate(), Collections.emptyList()));
        scheduleJob(job, 10);

        List<QuotaController<Job<?>>> controllers = buildQuotaControllers(job, jobOperations, titusRuntime);
        TaskRelocationLimitController controller = (TaskRelocationLimitController) controllers.get(0);

        Task task = jobOperations.getTasks(job.getId()).get(0);
        assertThat(controller.consume(task.getId()).isApproved()).isTrue();
        assertThat(controller.consume(task.getId()).isApproved()).isFalse();

        // Change job descriptor and consume some quota
        Job<BatchJobExt> updatedJob = jobComponentStub.changeJob(exceptPolicy(job, perTaskRelocationLimitPolicy(3)));
        List<QuotaController<Job<?>>> merged = mergeQuotaControllers(controllers, updatedJob, jobOperations, titusRuntime);
        TaskRelocationLimitController updatedController = (TaskRelocationLimitController) merged.get(0);

        assertThat(updatedController.consume(task.getId()).isApproved()).isTrue();
        assertThat(updatedController.consume(task.getId()).isApproved()).isTrue();
        assertThat(updatedController.consume(task.getId()).isApproved()).isFalse();
    }

    @Test
    public void testJobWithNoDisruptionBudgetHasZeroQuota() {
        Job<BatchJobExt> job = newBatchJob(10, DisruptionBudget.none());
        com.netflix.titus.api.model.reference.Reference jobReference = com.netflix.titus.api.model.reference.Reference.job(job.getId());

        scheduleJob(job, 10);
        JobQuotaController jobController = new JobQuotaController(job, jobOperations, containerHealthService, titusRuntime);

        assertThat(jobController.getQuota(jobReference).getQuota()).isEqualTo(0);
    }

    @Test
    public void testGetQuota() {
        Job<BatchJobExt> job = newBatchJob(10, budget(percentageOfHealthyPolicy(80.0), hourlyRatePercentage(50), singletonList(officeHourTimeWindow())));
        com.netflix.titus.api.model.reference.Reference jobReference = com.netflix.titus.api.model.reference.Reference.job(job.getId());

        scheduleJob(job, 10);
        JobQuotaController jobController = new JobQuotaController(job, jobOperations, containerHealthService, titusRuntime);

        assertThat(jobController.getQuota(jobReference).getQuota()).isEqualTo(2);

        clock.jumpForwardTo(DayOfWeek.SATURDAY);
        assertThat(jobController.getQuota(jobReference).getQuota()).isEqualTo(0);
    }

    @Test
    public void testConsume() {
        Job<BatchJobExt> job = newBatchJob(10, budget(percentageOfHealthyPolicy(80.0), hourlyRatePercentage(20), Collections.emptyList()));
        com.netflix.titus.api.model.reference.Reference jobReference = com.netflix.titus.api.model.reference.Reference.job(job.getId());

        scheduleJob(job, 10);
        JobQuotaController jobController = new JobQuotaController(job, jobOperations, containerHealthService, titusRuntime);

        assertThat(jobController.getQuota(jobReference).getQuota()).isEqualTo(2);

        Task task = jobOperations.getTasks(job.getId()).get(0);
        assertThat(jobController.consume(task.getId()).isApproved()).isTrue();
        assertThat(jobController.consume(task.getId()).isApproved()).isTrue();

        assertThat(jobController.getQuota(jobReference).getQuota()).isEqualTo(0);
        ConsumptionResult failure = jobController.consume(task.getId());
        assertThat(failure.isApproved()).isFalse();
        assertThat(failure.getRejectionReason().get()).contains("JobPercentagePerHourRelocationRateController");
    }

    @Test
    public void testUpdate() {
        Job<BatchJobExt> job = newBatchJob(10, budget(perTaskRelocationLimitPolicy(2), hourlyRatePercentage(20), Collections.emptyList()));
        com.netflix.titus.api.model.reference.Reference jobReference = com.netflix.titus.api.model.reference.Reference.job(job.getId());

        scheduleJob(job, 10);
        JobQuotaController jobController = new JobQuotaController(job, jobOperations, containerHealthService, titusRuntime);

        assertThat(jobController.getQuota(jobReference).getQuota()).isEqualTo(2);

        Task task = jobOperations.getTasks(job.getId()).get(0);
        assertThat(jobController.consume(task.getId()).isApproved()).isTrue();
        assertThat(jobController.consume(task.getId()).isApproved()).isTrue();
        assertThat(jobController.getQuota(jobReference).getQuota()).isEqualTo(0);

        // Now bump up the limit by 1
        Job<BatchJobExt> updatedJob = jobComponentStub.changeJob(
                exceptBudget(job, budget(perTaskRelocationLimitPolicy(3), hourlyRatePercentage(80), Collections.emptyList()))
        );
        JobQuotaController updatedController = jobController.update(updatedJob);

        assertThat(updatedController.consume(task.getId()).isApproved()).isTrue();
        assertThat(updatedController.consume(task.getId()).isApproved()).isFalse();
        assertThat(updatedController.getQuota(jobReference).getQuota()).isEqualTo(5); // 3 task killed out of 8 allowed in an hour

        // Now increase job size
        Job<BatchJobExt> scaledJob = jobComponentStub.changeJob(JobFunctions.changeBatchJobSize(updatedJob, 20));
        jobComponentStub.createDesiredTasks(scaledJob);

        JobQuotaController updatedController2 = jobController.update(scaledJob);
        assertThat(updatedController2.getQuota(jobReference).getQuota()).isEqualTo(13); // 3 task kills out of 16 allowed in an hour
    }

    private void checkContains(List<? extends QuotaTracker> trackers, Class<?>... expectedTypes) {
        Set<String> found = trackers.stream().map(t -> t.getClass().getSimpleName()).collect(Collectors.toSet());
        Set<String> expected = Stream.of(expectedTypes).map(Class::getSimpleName).collect(Collectors.toSet());
        assertThat(found).isEqualTo(expected);
    }

    private Job<?> scheduleJob(Job<?> job, int started) {
        jobComponentStub.createJob(job);
        List<Task> tasks = jobComponentStub.createDesiredTasks(job);
        for (int i = 0; i < started; i++) {
            jobComponentStub.moveTaskToState(tasks.get(i), TaskState.Started);
        }
        return job;
    }
}