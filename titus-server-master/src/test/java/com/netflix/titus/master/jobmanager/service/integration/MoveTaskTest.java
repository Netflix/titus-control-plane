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

package com.netflix.titus.master.jobmanager.service.integration;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.master.jobmanager.service.integration.scenario.JobScenarioBuilder;
import com.netflix.titus.master.jobmanager.service.integration.scenario.JobsScenarioBuilder;
import com.netflix.titus.master.jobmanager.service.integration.scenario.ScenarioTemplates;
import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Test;
import rx.schedulers.TestScheduler;

import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskServiceJobDescriptor;
import static org.assertj.core.api.Assertions.assertThat;

public class MoveTaskTest {

    private final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder();

    private final TestScheduler testScheduler = jobsScenarioBuilder.getTestScheduler();

    @Test
    public void testMove() {
        String targetJobId = startNewJob(oneTaskServiceJobDescriptor()).getJobId();

        startNewJob(oneTaskServiceJobDescriptor())
                .advance()
                .moveTask(0, 0, targetJobId)
                .expectJobEvent(job -> assertThat(JobFunctions.getJobDesiredSize(job)).isEqualTo(0));

        jobsScenarioBuilder.getJobScenario(0)
                .expectJobEvent(job -> assertThat(JobFunctions.getJobDesiredSize(job)).isEqualTo(2));
    }

    @Test
    public void testMoveWithInvalidTaskId() {
        ExtTestSubscriber<Void> testSubscriber = new ExtTestSubscriber<>();
        jobsScenarioBuilder.getJobOperations().moveServiceTask("badTaskId", "someJobId").subscribe(testSubscriber);

        assertThat(testSubscriber.isError()).isTrue();
        assertThat(((JobManagerException) testSubscriber.getError()).getErrorCode()).isEqualTo(JobManagerException.ErrorCode.TaskNotFound);
    }

    @Test
    public void testMoveWithBatchTask() {
        try {
            startNewJob(oneTaskBatchJobDescriptor()).moveTask(0, 0, "someJobId");
        } catch (JobManagerException e) {
            assertThat(e.getErrorCode()).isEqualTo(JobManagerException.ErrorCode.NotServiceJob);
        }
    }

    @Test
    public void testMoveWithInvalidTargetJob() {
        String targetJobId = startNewJob(oneTaskBatchJobDescriptor()).getJobId();
        try {
            startNewJob(oneTaskServiceJobDescriptor())
                    .advance()
                    .moveTask(0, 0, targetJobId)
                    .expectJobEvent(job -> assertThat(JobFunctions.getJobDesiredSize(job)).isEqualTo(0));
        } catch (JobManagerException e) {
            assertThat(e.getErrorCode()).isEqualTo(JobManagerException.ErrorCode.NotServiceJob);
        }
    }

    @Test
    public void testMoveWithStoreUpdateFailure() {
        String targetJobId = startNewJob(oneTaskServiceJobDescriptor()).getJobId();

        try {
            startNewJob(oneTaskServiceJobDescriptor())
                    .advance()
                    .breakStore()
                    .allTasks(tasks -> assertThat(tasks).hasSize(1))
                    .moveTask(0, 0, targetJobId);
        } catch (Exception e) {
            assertThat(ExceptionExt.toMessageChain(e)).contains("Store is broken");
        }

        jobsScenarioBuilder.getJobScenario(0).allTasks(tasks -> assertThat(tasks).hasSize(1));
        jobsScenarioBuilder.getJobScenario(1).allTasks(tasks -> assertThat(tasks).hasSize(1));
    }

    @Test
    public void testMoveTimeout() {
        String targetJobId = startNewJob(oneTaskServiceJobDescriptor()).getJobId();
        startNewJob(oneTaskServiceJobDescriptor())
                .advance()
                .slowStore()
                .inTask(0, 0, task -> {
                    ExtTestSubscriber<Void> testSubscriber = new ExtTestSubscriber<>();
                    jobsScenarioBuilder.getJobOperations()
                            .moveServiceTask(task.getId(), targetJobId)
                            .timeout(1, TimeUnit.SECONDS, testScheduler)
                            .subscribe(testSubscriber);
                    testScheduler.advanceTimeBy(2, TimeUnit.SECONDS);
                    assertThat(testSubscriber.isError()).isTrue();
                    assertThat(testSubscriber.getError()).isInstanceOf(TimeoutException.class);
                });
    }

    private <E extends JobDescriptor.JobDescriptorExt> JobScenarioBuilder<E> startNewJob(JobDescriptor<E> jobDescriptor) {
        jobsScenarioBuilder.scheduleJob(jobDescriptor, jobScenario -> jobScenario
                .template(ScenarioTemplates.acceptJobWithOneTask(0, 0))
                .template(ScenarioTemplates.startTask(0, 0, TaskState.Started))
        );
        return (JobScenarioBuilder<E>) CollectionsExt.last(jobsScenarioBuilder.getJobScenarios());
    }
}
