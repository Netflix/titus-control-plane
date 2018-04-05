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

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.master.jobmanager.service.integration.scenario.JobsScenarioBuilder;
import com.netflix.titus.master.jobmanager.service.integration.scenario.ScenarioTemplates;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JobSchedulingCommonTest {

    private final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder();

    @Test
    public void testBatchJobWithTaskInAcceptedStateNotScheduledYet() {
        testJobWithTaskInAcceptedStateNotScheduledYet(JobDescriptorGenerator.oneTaskBatchJobDescriptor());
    }

    @Test
    public void testServiceJobWithTaskInAcceptedStateNotScheduledYet() {
        testJobWithTaskInAcceptedStateNotScheduledYet(JobDescriptorGenerator.oneTaskServiceJobDescriptor());
    }

    /**
     * This test covers the case where a task is created in store, but not added to Fenzo yet, and the job while in this
     * state is terminated.
     */
    private void testJobWithTaskInAcceptedStateNotScheduledYet(JobDescriptor<?> oneTaskJobDescriptor) {
        jobsScenarioBuilder.scheduleJob(oneTaskJobDescriptor, jobScenario -> jobScenario
                .expectJobEvent()
                .expectTaskAddedToStore(0, 0, task -> assertThat(task.getStatus().getState()).isEqualTo(TaskState.Accepted))
                .template(ScenarioTemplates.killJob())
                .expectTaskStateChangeEvent(0, 0, TaskState.Accepted)
                .template(ScenarioTemplates.handleTaskFinishedTransitionInSingleTaskJob(0, 0, TaskStatus.REASON_TASK_KILLED))
        );
    }

    @Test
    public void testTryToLaunchTaskWhichIsInFinishedState() {
        jobsScenarioBuilder.scheduleJob(JobDescriptorGenerator.oneTaskServiceJobDescriptor(), jobScenario -> jobScenario
                .expectJobEvent()
                .expectTaskStateChangeEvent(0, 0, TaskState.Accepted)
                .expectScheduleRequest(0, 0)
                .killTask(0, 0)
                .expectTaskStateChangeEvent(0, 0, TaskState.KillInitiated)
                .triggerMesosFinishedEvent(0, 0, -1, TaskStatus.REASON_TASK_LOST)
                .triggerFailingSchedulerLaunchEvent(0, 0, error -> assertThat(error).isInstanceOf(JobManagerException.class))
                .advance() // Advance to observe 'status=error type=afterChange' in the log file
        );
    }
}
