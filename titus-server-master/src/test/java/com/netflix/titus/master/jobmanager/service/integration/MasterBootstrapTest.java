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

import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.master.jobmanager.service.integration.scenario.JobsScenarioBuilder;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import org.junit.Test;

import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.changeServiceJobCapacity;
import static org.assertj.core.api.Assertions.assertThat;

public class MasterBootstrapTest {

    private final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder();

    @Test
    public void testBatchTaskStuckInLaunchedStateBeforeFailover() {
        testTaskStuckInLaunchedStateBeforeFailover(JobDescriptorGenerator.oneTaskBatchJobDescriptor());
    }

    @Test
    public void testServiceTaskStuckInLaunchedStateBeforeFailover() {
        testTaskStuckInLaunchedStateBeforeFailover(JobDescriptorGenerator.oneTaskServiceJobDescriptor());
    }

    private void testTaskStuckInLaunchedStateBeforeFailover(JobDescriptor<?> jobDescriptor) {
        jobsScenarioBuilder.scheduleJob(jobDescriptor, jobScenario -> jobScenario
                .expectJobEvent()
                .expectTaskAddedToStore(0, 0, task -> assertThat(task.getStatus().getState()).isEqualTo(TaskState.Accepted))
                .expectTaskStateChangeEvent(0, 0, TaskState.Accepted)
                .expectScheduleRequest(0, 0)
                .triggerSchedulerLaunchEvent(0, 0)
                .expectTaskStateChangeEvent(0, 0, TaskState.Launched)
                .advance(JobsScenarioBuilder.LAUNCHED_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .breakStore()
                .advance().advance()
                .enableStore()
        ).reboot()
                .inJob(0, jobScenario -> jobScenario
                        .expectTaskInActiveState(0, 0, TaskState.Launched)
                        .advance(JobsScenarioBuilder.LAUNCHED_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                        .advance()
                        .expectTaskInActiveState(0, 0, TaskState.KillInitiated)
                );
    }

    /**
     * This test passes because by default {@link com.netflix.titus.master.jobmanager.service.JobManagerConfiguration#isFailOnDataValidation}
     * is turned off.
     */
    @Test
    public void testRebootWithJobHavingNegativeDesiredSize() {
        JobDescriptor<ServiceJobExt> emptyJob = changeServiceJobCapacity(JobDescriptorGenerator.oneTaskServiceJobDescriptor(), Capacity.newBuilder().build());

        jobsScenarioBuilder.scheduleJob(emptyJob, jobScenario -> jobScenario
                .expectJobEvent()
                .modifyJobStoreRecord(jobStoreRecord -> {
                    return changeServiceJobCapacity(jobStoreRecord, Capacity.newBuilder().withMin(-1).withDesired(-1).build());
                })
        ).reboot()
                .inJob(0, jobScenario -> jobScenario
                        .assertServiceJob(job -> {
                            assertThat(job.getJobDescriptor().getExtensions().getCapacity().getDesired()).isEqualTo(-1);
                        })
                );
    }
}
