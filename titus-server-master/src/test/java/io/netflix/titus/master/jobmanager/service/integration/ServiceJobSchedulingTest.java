/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.jobmanager.service.integration;

import java.util.function.Consumer;

import io.netflix.titus.api.jobmanager.model.job.Capacity;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.JobFunctions;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.master.jobmanager.service.integration.scenario.JobsScenarioBuilder;
import io.netflix.titus.master.jobmanager.service.integration.scenario.ScenarioTemplates;
import org.junit.Test;

import static io.netflix.titus.api.jobmanager.model.job.JobFunctions.changeServiceJobCapacity;
import static io.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskServiceJobDescriptor;
import static org.assertj.core.api.Assertions.assertThat;

public class ServiceJobSchedulingTest {

    private final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder();

    /**
     * Run single service task that terminates with exit code 0. The task should be resubmitted.
     * When the job is killed, its task is killed as well.
     */
    @Test
    public void testTaskCompletingOkIsResubmitted() {
        testTaskCompletingOkIsResubmitted(0);
    }

    /**
     * Run single service task that terminates with exit code 0. The task should be resubmitted.
     * When the job is killed, its task is killed as well.
     */
    @Test
    public void testFailingTaskIsResubmitted() {
        testTaskCompletingOkIsResubmitted(-1);
    }

    private void testTaskCompletingOkIsResubmitted(int errorCode) {
        jobsScenarioBuilder.scheduleJob(oneTaskServiceJobDescriptor(), jobScenario -> jobScenario
                .template(ScenarioTemplates.acceptJobWithOneTask(0, 0))
                .template(ScenarioTemplates.startTask(0, 0, TaskState.Started))
                .template(ScenarioTemplates.triggerMesosFinishedEvent(0, 0, errorCode))
                .template(ScenarioTemplates.acceptTask(0, 1))
                .template(ScenarioTemplates.killJob())
                .template(ScenarioTemplates.reconcilerTaskKill(0, 1))
                .template(ScenarioTemplates.handleTaskFinishedTransitionInSingleTaskJob(0, 1, TaskStatus.REASON_TASK_KILLED))
        );
    }

    /**
     * Run service with multiple tasks that terminate with exit code 0. The tasks should be resubmitted.
     * When the job is killed, all tasks are killed as well.
     */
    @Test
    public void testAllTasksInJobAreResubmittedWhenCompleteOk() {
        JobDescriptor<ServiceJobExt> twoTaskJob = changeServiceJobCapacity(oneTaskServiceJobDescriptor(), 2);
        jobsScenarioBuilder.scheduleJob(twoTaskJob, jobScenario -> jobScenario
                .expectJobEvent()
                .advance()
                .inActiveTasks((taskIdx, resubmit) -> ScenarioTemplates.acceptTask(taskIdx, resubmit))
                .inActiveTasks((taskIdx, resubmit) -> ScenarioTemplates.startTask(taskIdx, resubmit, TaskState.Started))
                .inActiveTasks((taskIdx, resubmit) -> ScenarioTemplates.triggerMesosFinishedEvent(taskIdx, resubmit, 0))
                .advance().advance()
                .inActiveTasks((taskIdx, resubmit) -> ScenarioTemplates.acceptTask(taskIdx, resubmit))
                .template(ScenarioTemplates.killJob())
                .inActiveTasks((taskIdx, resubmit) -> js -> js
                        .template(ScenarioTemplates.reconcilerTaskKill(taskIdx, resubmit))
                        .expectTaskUpdatedInStore(taskIdx, resubmit, task -> assertThat(task.getStatus().getState()).isEqualTo(TaskState.Finished))
                )
                .template(ScenarioTemplates.verifyJobWithFinishedTasksCompletes())
        );
    }

    @Test
    public void testZeroSizeJobIsNotCompletedAutomatically() {
        JobDescriptor<?> emptyJob = JobFunctions.changeServiceJobCapacity(oneTaskServiceJobDescriptor(), 0);
        jobsScenarioBuilder.scheduleJob(emptyJob, jobScenario -> jobScenario
                .expectJobEvent()
                .advance()
                .expectNoStoreUpdate()
                .expectNoJobStateChangeEvent()
        );
    }

    @Test
    public void testJobScaleUp() {
        Capacity newCapacity = Capacity.newBuilder().withMin(0).withDesired(2).withMax(5).build();

        jobsScenarioBuilder.scheduleJob(oneTaskServiceJobDescriptor(), jobScenario -> jobScenario
                .template(ScenarioTemplates.acceptJobWithOneTask(0, 0))
                .template(ScenarioTemplates.changeJobCapacity(newCapacity))
                .advance()
                .expectTaskInActiveState(1, 0, TaskState.Accepted)
        );
    }

    @Test
    public void testJobScaleDown() {
        Capacity newCapacity = Capacity.newBuilder().withMin(0).withDesired(1).withMax(5).build();
        JobDescriptor<ServiceJobExt> twoTaskJob = changeServiceJobCapacity(oneTaskServiceJobDescriptor(), 2);
        jobsScenarioBuilder.scheduleJob(twoTaskJob, jobScenario -> jobScenario
                .expectJobEvent()
                .advance()
                .inActiveTasks((taskIdx, resubmit) -> ScenarioTemplates.acceptTask(taskIdx, resubmit))
                .template(ScenarioTemplates.changeJobCapacity(newCapacity))
                .advance()
                .firstTaskMatch(task -> task.getStatus().getState() == TaskState.KillInitiated, matchingTask -> {
                    assertThat(matchingTask.getStatus().getReasonCode()).isEqualTo(TaskStatus.REASON_SCALED_DOWN);
                })
        );
    }

    @Test
    public void testTaskTerminateInAcceptedState() {
        jobsScenarioBuilder.scheduleJob(oneTaskServiceJobDescriptor(), jobScenario -> jobScenario
                .expectJobEvent()
                .advance()
                .template(ScenarioTemplates.acceptTask(0, 0))
                .killTask(0, 0)
                .expectTaskStateChangeEvent(0, 0, TaskState.KillInitiated)
                .triggerMesosFinishedEvent(0, 0, -1, TaskStatus.REASON_TASK_LOST)
                .expectTaskStateChangeEvent(0, 0, TaskState.Finished)
        );
    }

    @Test
    public void testTaskTerminateAndShrinkReducesJobSize() {
        JobDescriptor<ServiceJobExt> twoTaskJob = changeServiceJobCapacity(oneTaskServiceJobDescriptor(), 3);
        jobsScenarioBuilder.scheduleJob(twoTaskJob, jobScenario -> jobScenario
                .expectJobEvent()
                .advance()
                .inActiveTasks((taskIdx, resubmit) -> ScenarioTemplates.acceptTask(taskIdx, resubmit))
                .killTaskAndShrink(0, 0)
                .expectTaskStateChangeEvent(0, 0, TaskState.KillInitiated)
                .expectTaskUpdatedInStore(0, 0, task -> assertThat(task.getStatus().getState()).isEqualTo(TaskState.KillInitiated))
                .triggerMesosFinishedEvent(0, 0, -1, TaskStatus.REASON_TASK_KILLED)
                .expectTaskStateChangeEvent(0, 0, TaskState.Finished, TaskStatus.REASON_TASK_KILLED)
                .expectTaskUpdatedInStore(0, 0, task -> assertThat(task.getStatus().getState()).isEqualTo(TaskState.Finished))
                .expectedTaskArchivedInStore(0, 0)
                .advance()
                .advance()
                .expectNoTaskStateChangeEvent()
                .expectServiceJobEvent(job -> assertThat(job.getJobDescriptor().getExtensions().getCapacity().getDesired()).isEqualTo(2))
        );
    }

    @Test
    public void testJobCapacityUpdateToIdenticalAsCurrentCapacityIsNoOp() {
        Capacity fixedCapacity = Capacity.newBuilder().withMin(1).withDesired(1).withMax(1).build();
        JobDescriptor<ServiceJobExt> job = JobFunctions.changeServiceJobCapacity(oneTaskServiceJobDescriptor(), fixedCapacity);

        jobsScenarioBuilder.scheduleJob(job, jobScenario -> jobScenario
                .template(ScenarioTemplates.acceptJobWithOneTask(0, 0))
                .changeCapacity(fixedCapacity)
                .advance()
                .advance()
                .expectNoStoreUpdate()
                .expectNoJobStateChangeEvent()
                .expectNoTaskStateChangeEvent()
        );
    }

    @Test
    public void testJobCapacityUpdateWhenJobInKillInitiatedStateIsIgnored() {
        Capacity newCapacity = Capacity.newBuilder().withMin(0).withDesired(2).withMax(5).build();

        jobsScenarioBuilder.scheduleJob(oneTaskServiceJobDescriptor(), jobScenario -> jobScenario
                .template(ScenarioTemplates.acceptJobWithOneTask(0, 0))
                .template(ScenarioTemplates.killJob())
                .template(ScenarioTemplates.changeJobCapacity(newCapacity))
                .advance()
                .advance()
                .expectNoJobStateChangeEvent()
        );
    }

    @Test
    public void testJobEnableStatus() {
        jobsScenarioBuilder.scheduleJob(oneTaskServiceJobDescriptor(), jobScenario -> {
                    Consumer<Boolean> enable = enabled -> jobScenario
                            .changeJobEnabledStatus(enabled)
                            .expectJobEvent(job -> assertJobEnableState(job, enabled));
                    return jobScenario
                            .expectJobEvent()
                            .assertServiceJob(job -> assertJobEnableState(job, true))
                            .andThen(() -> enable.accept(false))
                            .andThen(() -> enable.accept(true))
                            .andThen(() -> enable.accept(false));
                }
        );
    }

    @Test
    public void testJobEnableStatusUpdateToIdenticalValue() {
        JobDescriptor<?> emptyJob = JobFunctions.changeServiceJobCapacity(oneTaskServiceJobDescriptor(), 0);
        jobsScenarioBuilder.scheduleJob(emptyJob, jobScenario -> jobScenario
                .expectJobEvent()
                .assertServiceJob(job -> assertJobEnableState(job, true))
                .changeJobEnabledStatus(true)
                .advance()
                .expectNoStoreUpdate()
                .expectNoJobStateChangeEvent()
        );
    }

    private void assertJobEnableState(Job<?> job, boolean enabled) {
        Job<ServiceJobExt> serviceJob = (Job<ServiceJobExt>) job;
        assertThat(serviceJob.getJobDescriptor().getExtensions().isEnabled()).describedAs("Expecting job in enable state: %s", enabled).isEqualTo(enabled);
    }
}
