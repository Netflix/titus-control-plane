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
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.master.jobmanager.service.integration.scenario.JobScenarioBuilder;
import com.netflix.titus.master.jobmanager.service.integration.scenario.JobsScenarioBuilder;
import com.netflix.titus.master.jobmanager.service.integration.scenario.ScenarioTemplates;
import org.junit.Test;

import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.changeServiceJobCapacity;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskServiceJobDescriptor;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Run tests with Kube integration.
 * <p>
 * TODO We should run all integration tests twice. Once with Mesos and once with Kube integration.
 */
public class KubeSchedulerTest {

    private final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder(true);

    @Test
    public void testRunAndCompleteOkBatchJob() {
        jobsScenarioBuilder.scheduleJob(oneTaskBatchJobDescriptor(), jobScenario -> jobScenario
                .template(ScenarioTemplates.acceptJobWithOneTask(0, 0))
                .template(ScenarioTemplates.startTask(0, 0, TaskState.Started))
                .template(ScenarioTemplates.finishSingleTaskJob(0, 0, TaskStatus.REASON_NORMAL, 0))
        );
    }

    @Test
    public void testRunAndCompleteOkServiceJob() {
        JobDescriptor<ServiceJobExt> twoTaskJob = changeServiceJobCapacity(oneTaskServiceJobDescriptor(), 2);
        jobsScenarioBuilder.scheduleJob(twoTaskJob, jobScenario -> jobScenario
                .expectJobEvent()
                .advance()
                .inActiveTasks((taskIdx, resubmit) -> ScenarioTemplates.acceptTask(taskIdx, resubmit))
                .inActiveTasks((taskIdx, resubmit) -> ScenarioTemplates.startTask(taskIdx, resubmit, TaskState.Started))
                .inActiveTasks((taskIdx, resubmit) -> ScenarioTemplates.triggerMesosFinishedEvent(taskIdx, resubmit, 0))
                .advance().advance()
                .inActiveTasks((taskIdx, resubmit) -> ScenarioTemplates.acceptTask(taskIdx, resubmit))
                .ignoreAvailableEvents()
                .template(ScenarioTemplates.killJob())
                .advance()
                .inActiveTasks((taskIdx, resubmit) -> js -> js
                        .template(ScenarioTemplates.reconcilerTaskKill(taskIdx, resubmit))
                        .expectTaskUpdatedInStore(taskIdx, resubmit, task -> assertThat(task.getStatus().getState()).isEqualTo(TaskState.Finished))
                )
                .template(ScenarioTemplates.verifyJobWithFinishedTasksCompletes())
        );
    }

    @Test
    public void testBatchTaskTerminate() {
        jobsScenarioBuilder.scheduleJob(oneTaskBatchJobDescriptor(), jobScenario -> jobScenario
                .template(ScenarioTemplates.acceptJobWithOneTask(0, 0))
                .template(ScenarioTemplates.startTask(0, 0, TaskState.Started))
                .template(ScenarioTemplates.killKubeTask(0, 0))
        );
    }

    @Test
    public void testBatchPodCreateFailure() {
        JobScenarioBuilder<JobDescriptor.JobDescriptorExt> js = jobsScenarioBuilder.scheduleJob(oneTaskBatchJobDescriptor(), jobScenario -> jobScenario
                .failNextPodCreate(new IllegalStateException("simulated pod create error"))
                .expectJobEvent()
                .expectTaskStateChangeEvent(0, 0, TaskState.Accepted)
                .allTasks(allTasks -> assertThat(allTasks).hasSize(1))
                .advance()
                .expectTaskStateChangeEvent(0, 0, TaskState.Finished)
        ).getJobScenario(0);

        for (int resubmit = 1; resubmit < 10; resubmit++) {
            js.failNextPodCreate(new IllegalStateException("simulated pod create error"))
                    .advance()
                    .expectTaskStateChangeEvent(0, resubmit, TaskState.Accepted)
                    .allTasks(allTasks -> assertThat(allTasks).hasSize(1))
                    .advance()
                    .expectTaskStateChangeEvent(0, resubmit, TaskState.Finished)
                    .advance();
        }
    }

    @Test
    public void testServicePodCreateFailure() {
        jobsScenarioBuilder.scheduleJob(oneTaskServiceJobDescriptor(), jobScenario -> jobScenario
                .failNextPodCreate(new IllegalStateException("simulated pod create error"))
                .expectJobEvent()
                .expectTaskStateChangeEvent(0, 0, TaskState.Accepted)
                .advance()
                .expectTaskStateChangeEvent(0, 0, TaskState.Finished)
                .advance()
                .expectTaskStateChangeEvent(0, 1, TaskState.Accepted)
                .allTasks(allTasks -> assertThat(allTasks).hasSize(1))
        );
    }
}
