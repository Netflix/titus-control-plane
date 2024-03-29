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
import java.util.function.Consumer;

import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobProcesses;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.common.util.code.RecordingCodeInvariants;
import com.netflix.titus.master.jobmanager.service.integration.scenario.JobScenarioBuilder;
import com.netflix.titus.master.jobmanager.service.integration.scenario.JobsScenarioBuilder;
import com.netflix.titus.master.jobmanager.service.integration.scenario.ScenarioTemplates;
import org.junit.After;
import org.junit.Test;

import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.changeServiceJobCapacity;
import static com.netflix.titus.master.jobmanager.service.integration.scenario.JobsScenarioBuilder.CONCURRENT_STORE_UPDATE_LIMIT;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskServiceJobDescriptor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class ServiceJobSchedulingTest {

    private final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder();

    @After
    public void tearDown() {
        RecordingCodeInvariants invariants = (RecordingCodeInvariants) jobsScenarioBuilder.getTitusRuntime().getCodeInvariants();
        assertThat(invariants.getViolations()).describedAs("Invariant violations found").isEmpty();
    }

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
                .template(ScenarioTemplates.triggerComputeProviderFinishedEvent(0, 0, errorCode))
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
                .inActiveTasks((taskIdx, resubmit) -> ScenarioTemplates.triggerComputeProviderFinishedEvent(taskIdx, resubmit, 0))
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

    @Test(expected = JobManagerException.class)
    public void testFinishedJobScaleUp() {
        Capacity newCapacity = Capacity.newBuilder().withMin(0).withDesired(2).withMax(5).build();
        testNoUpdatesAllowedForFinishedJob(jobScenario -> jobScenario
                .template(ScenarioTemplates.changeJobCapacity(newCapacity))
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
    public void testJobScaleDownWithTaskInKillInitiatedState() {
        Capacity newCapacity = Capacity.newBuilder().withMin(0).withDesired(0).withMax(5).build();
        jobsScenarioBuilder.scheduleJob(oneTaskServiceJobDescriptor(), jobScenario -> jobScenario
                .template(ScenarioTemplates.acceptJobWithOneTask(0, 0))
                .killTask(0, 0)
                .template(ScenarioTemplates.changeJobCapacity(newCapacity))
                .expectTaskStateChangeEvent(0, 0, TaskState.KillInitiated)
                .triggerComputePlatformFinishedEvent(0, 0)
                .expectTaskStateChangeEvent(0, 0, TaskState.Finished)
                .advance().advance().advance()
                .allActiveTasks(task -> fail("No active task expected, but found: " + task))
        );
    }

    @Test
    public void testJobScaleDownWithParallelTerminateAndShrink() {
        JobDescriptor<ServiceJobExt> twoTaskJob = changeServiceJobCapacity(oneTaskServiceJobDescriptor(), 2);
        Capacity newCapacity = Capacity.newBuilder().withMin(0).withDesired(0).withMax(5).build();

        jobsScenarioBuilder.withConcurrentStoreUpdateLimit(1)
                .scheduleJob(twoTaskJob, jobScenario -> jobScenario
                        .expectJobEvent()
                        .advance()
                        .inActiveTasks((taskIdx, resubmit) -> ScenarioTemplates.acceptTask(taskIdx, resubmit))
                        .inActiveTasks((taskIdx, resubmit) -> ScenarioTemplates.startTask(taskIdx, resubmit, TaskState.Started))
                        .changeCapacity(newCapacity)
                        .allTasks(tasks -> tasks.forEach(jobScenario::killTaskAndShrinkNoWait))
                        .advance().advance().advance()
                        .assertServiceJob(job -> {
                            Capacity capacity = job.getJobDescriptor().getExtensions().getCapacity();
                            assertThat(capacity.getMin()).isEqualTo(0);
                            assertThat(capacity.getDesired()).isEqualTo(0);
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
                .triggerComputePlatformFinishedEvent(0, 0, -1, TaskStatus.REASON_TASK_LOST)
                .expectTaskStateChangeEvent(0, 0, TaskState.Finished)
        );
    }

    @Test
    public void testTaskTerminateInAcceptedStateKillRetries() {
        jobsScenarioBuilder.scheduleJob(oneTaskServiceJobDescriptor(), jobScenario -> jobScenario
                .expectJobEvent()
                .advance()
                .template(ScenarioTemplates.acceptTask(0, 0))
                .killTask(0, 0)
                .expectComputeProviderTaskFinished(0, 0)
                .expectTaskStateChangeEvent(0, 0, TaskState.KillInitiated)
                .advance(2 * JobsScenarioBuilder.KILL_INITIATED_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .expectComputeProviderTaskFinished(0, 0)
                .expectTaskStateChangeEvent(0, 0, TaskState.KillInitiated)
                .triggerComputePlatformFinishedEvent(0, 0, -1, TaskStatus.REASON_TASK_LOST)
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
                .triggerComputePlatformFinishedEvent(0, 0, -1, TaskStatus.REASON_TASK_KILLED)
                .expectTaskStateChangeEvent(0, 0, TaskState.Finished, TaskStatus.REASON_TASK_KILLED)
                .expectTaskUpdatedInStore(0, 0, task -> assertThat(task.getStatus().getState()).isEqualTo(TaskState.Finished))
                .expectedTaskArchivedInStore(0, 0)
                .expectArchivedTaskEvent(0, 0)
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
    public void testJobKillWithTaskInAcceptedStateWithRetries() {
        jobsScenarioBuilder.scheduleJob(oneTaskServiceJobDescriptor(), jobScenario -> jobScenario
                .template(ScenarioTemplates.acceptJobWithOneTask(0, 0))
                .template(ScenarioTemplates.killJob())
                .expectComputeProviderTaskFinished(0, 0)
                .expectTaskStateChangeEvent(0, 0, TaskState.KillInitiated)
                .advance(2 * JobsScenarioBuilder.KILL_INITIATED_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .expectComputeProviderTaskFinished(0, 0)
                .expectTaskStateChangeEvent(0, 0, TaskState.KillInitiated)
                .triggerComputePlatformFinishedEvent(0, 0, -1, TaskStatus.REASON_TASK_LOST)
                .expectTaskStateChangeEvent(0, 0, TaskState.Finished)
                .advance().advance()
                .expectServiceJobEvent(job -> assertThat(job.getStatus().getState() == JobState.Finished))
        );
    }

    @Test
    public void testTaskKillConcurrencyIsLimitedWhenJobIsKilled() {
        jobsScenarioBuilder.scheduleJob(JobFunctions.changeServiceJobCapacity(oneTaskServiceJobDescriptor(), 100), jobScenario -> jobScenario
                .expectJobEvent()
                .advance()
                .inActiveTasks((taskIdx, resubmit) -> ScenarioTemplates.acceptTask(taskIdx, resubmit))
                .inActiveTasks((taskIdx, resubmit) -> ScenarioTemplates.startTask(taskIdx, resubmit, TaskState.Started))
                .advance()
                .allTasks(tasks -> assertThat(tasks.size() > CONCURRENT_STORE_UPDATE_LIMIT).isTrue())
                .ignoreAvailableEvents()
                .killJob()
                .expectJobEvent(job -> assertThat(job.getStatus().getState()).isEqualTo(JobState.KillInitiated))
                .advance()
                .allTasks(tasks -> {
                    long killed = tasks.stream().filter(task -> task.getStatus().getState() == TaskState.KillInitiated).count();
                    assertThat(killed).isEqualTo(CONCURRENT_STORE_UPDATE_LIMIT);
                })
        );
    }

    @Test
    public void testTaskKillConcurrencyIsLimitedWhenJobIsScaledDown() {
        jobsScenarioBuilder.scheduleJob(JobFunctions.changeServiceJobCapacity(oneTaskServiceJobDescriptor(), 100), jobScenario -> jobScenario
                .expectJobEvent()
                .advance()
                .inActiveTasks((taskIdx, resubmit) -> ScenarioTemplates.acceptTask(taskIdx, resubmit))
                .inActiveTasks((taskIdx, resubmit) -> ScenarioTemplates.startTask(taskIdx, resubmit, TaskState.Started))
                .advance()
                .allTasks(tasks -> assertThat(tasks.size() > CONCURRENT_STORE_UPDATE_LIMIT).isTrue())
                .ignoreAvailableEvents()
                .changeCapacity(0, 0, 0)
                .advance()
                .allTasks(tasks -> {
                    long killed = tasks.stream().filter(task -> task.getStatus().getState() == TaskState.KillInitiated).count();
                    assertThat(killed).isEqualTo(CONCURRENT_STORE_UPDATE_LIMIT);
                })
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

    @Test(expected = JobManagerException.class)
    public void testFinishedJobEnableStatus() {
        testNoUpdatesAllowedForFinishedJob(jobScenario -> jobScenario.changeJobEnabledStatus(false));
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

    @Test(expected = JobManagerException.class)
    public void testFinishedJobServiceProcessesUpdate() {
        testNoUpdatesAllowedForFinishedJob(jobScenario -> jobScenario
                .changServiceJobProcesses(ServiceJobProcesses.newBuilder().withDisableIncreaseDesired(true).build())
        );
    }

    private void testNoUpdatesAllowedForFinishedJob(Consumer<JobScenarioBuilder> changeFun) {
        JobDescriptor<ServiceJobExt> zeroSizeJob = oneTaskServiceJobDescriptor().but(JobFunctions.ofServiceSize(0));
        jobsScenarioBuilder.scheduleJob(zeroSizeJob, jobScenario -> {
                    jobScenario
                            .ignoreAvailableEvents()
                            .killJob()
                            .expectJobEvent(job -> assertThat(job.getStatus().getState()).isEqualTo(JobState.KillInitiated))
                            .expectJobEvent(job -> assertThat(job.getStatus().getState()).isEqualTo(JobState.Finished))
                            .ignoreAvailableEvents();
                    changeFun.accept(jobScenario);
                    return jobScenario;
                }
        );
    }

    private void assertJobEnableState(Job<?> job, boolean enabled) {
        Job<ServiceJobExt> serviceJob = (Job<ServiceJobExt>) job;
        assertThat(serviceJob.getJobDescriptor().getExtensions().isEnabled()).describedAs("Expecting job in enable state: %s", enabled).isEqualTo(enabled);
    }
}
