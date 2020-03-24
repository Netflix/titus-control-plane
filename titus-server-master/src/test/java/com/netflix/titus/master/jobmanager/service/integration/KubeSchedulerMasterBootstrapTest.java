/*
 * Copyright 2020 Netflix, Inc.
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
import com.netflix.titus.master.jobmanager.service.integration.scenario.JobsScenarioBuilder;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class KubeSchedulerMasterBootstrapTest {

    private final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder(true);

    @Test
    public void testRestartWithBasicTaskAcceptedWithoutPod() {
        testRestartWithTaskAcceptedWithoutPod(JobDescriptorGenerator.oneTaskBatchJobDescriptor());
    }

    @Test
    public void testRestartWithServiceTaskAcceptedWithoutPod() {
        testRestartWithTaskAcceptedWithoutPod(JobDescriptorGenerator.oneTaskServiceJobDescriptor());
    }

    private void testRestartWithTaskAcceptedWithoutPod(JobDescriptor<?> jobDescriptor) {
        jobsScenarioBuilder.scheduleJob(jobDescriptor, jobScenario -> jobScenario
                .expectJobEvent()
                .expectTaskAddedToStore(0, 0, task -> assertThat(task.getStatus().getState()).isEqualTo(TaskState.Accepted))
        ).reboot()
                .inJob(0, jobScenario -> jobScenario
                        .expectTaskInActiveState(0, 0, TaskState.Accepted)
                        .advance()
                        .expectScheduleRequest(0, 0)
                );
    }
}
