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

package com.netflix.titus.master.jobmanager.service.integration.scenario;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor.JobDescriptorExt;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;

import static com.netflix.titus.master.jobmanager.service.integration.scenario.JobScenarioBuilder.CHANGE_CAPACITY_CALL_METADATA;
import static org.assertj.core.api.Assertions.assertThat;

public class ScenarioTemplates {

    public static Function<JobScenarioBuilder, JobScenarioBuilder> changeJobCapacity(Capacity newCapacity) {
        return jobScenario -> jobScenario
                .changeCapacity(newCapacity)
                .expectJobUpdateEventObject(event -> {
                    Job<ServiceJobExt> job = event.getCurrent();
                    assertThat(job.getJobDescriptor().getExtensions().getCapacity()).isEqualTo(newCapacity);
                    assertThat(event.getCallMetadata()).isEqualTo(CHANGE_CAPACITY_CALL_METADATA);
                })
                .expectServiceJobUpdatedInStore(job -> assertThat(job.getJobDescriptor().getExtensions().getCapacity()).isEqualTo(newCapacity));
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder, JobScenarioBuilder> acceptJobWithOneTask(int taskIdx, int resubmit) {
        return jobScenario -> jobScenario
                .expectJobEvent()
                .template(acceptTask(taskIdx, resubmit));
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder, JobScenarioBuilder> acceptTask(int taskIdx, int resubmit) {
        return jobScenario -> jobScenario
                .expectTaskAddedToStore(taskIdx, resubmit, task -> {
                    assertThat(task.getStatus().getState()).isEqualTo(TaskState.Accepted);
                    assertThat(task.getTaskContext())
                            .containsEntry(TaskAttributes.TASK_ATTRIBUTES_CELL, JobDescriptorGenerator.TEST_CELL_NAME);
                    assertThat(task.getTaskContext())
                            .containsEntry(TaskAttributes.TASK_ATTRIBUTES_STACK, JobDescriptorGenerator.TEST_STACK_NAME);
                })
                .expectTaskStateChangeEvent(taskIdx, resubmit, TaskState.Accepted, "normal")
                .expectComputeProviderCreateRequest(taskIdx, resubmit);
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder, JobScenarioBuilder> triggerComputePlatformStartInitiatedEvent(int taskIdx, int resubmit) {
        return jobScenario -> jobScenario
                .triggerComputePlatformStartInitiatedEvent(taskIdx, resubmit)
                .expectTaskUpdatedInStore(taskIdx, resubmit, task -> {
                    assertThat(task.getStatus().getState()).isEqualTo(TaskState.StartInitiated);
                    assertThat(task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP)).isNotEmpty();
                })
                .expectTaskStateChangeEvent(taskIdx, resubmit, TaskState.StartInitiated);
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder, JobScenarioBuilder> triggerComputeProviderStartedEvent(int taskIdx, int resubmit) {
        return jobScenario -> jobScenario
                .triggerComputePlatformStartedEvent(taskIdx, resubmit)
                .expectTaskUpdatedInStore(taskIdx, resubmit, task -> assertThat(task.getStatus().getState()).isEqualTo(TaskState.Started))
                .expectTaskStateChangeEvent(taskIdx, resubmit, TaskState.Started);
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder, JobScenarioBuilder> triggerComputeProviderFinishedEvent(int taskIdx, int resubmit, int errorCode) {
        String reasonCode = errorCode == 0 ? TaskStatus.REASON_NORMAL : TaskStatus.REASON_FAILED;
        return jobScenario -> jobScenario
                .triggerComputePlatformFinishedEvent(taskIdx, resubmit, errorCode, reasonCode)
                .expectTaskUpdatedInStore(taskIdx, resubmit, task -> {
                    assertThat(task.getStatus().getState()).isEqualTo(TaskState.Finished);
                    assertThat(task.getStatus().getReasonCode()).isEqualTo(reasonCode);
                })
                .expectTaskStateChangeEvent(taskIdx, resubmit, TaskState.Finished, reasonCode);

    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder, JobScenarioBuilder> startTask(int taskIdx, int resubmit, TaskState targetTaskState) {
        return jobScenario -> {
            JobScenarioBuilder accepted = jobScenario.expectTaskInActiveState(taskIdx, resubmit, TaskState.Accepted);
            if (targetTaskState == TaskState.Accepted) {
                return accepted;
            }

            jobScenario.inTask(taskIdx, resubmit, task -> jobScenario.getComputeProvider().scheduleTask(task.getId()));

            JobScenarioBuilder launched = jobScenario.triggerComputePlatformLaunchEvent(taskIdx, resubmit);
            jobScenario.advance();
            jobScenario.expectTaskUpdatedInStore(taskIdx, resubmit, updatedTask -> assertThat(updatedTask.getStatus().getState() == TaskState.Launched));
            jobScenario.expectTaskEvent(taskIdx, resubmit, event -> assertThat(event.getCurrentTask().getStatus().getState() == TaskState.Launched));
            if (targetTaskState == TaskState.Launched) {
                return launched;
            }

            JobScenarioBuilder startInitiated = launched.template(triggerComputePlatformStartInitiatedEvent(taskIdx, resubmit));
            if (targetTaskState == TaskState.StartInitiated) {
                return startInitiated;
            }

            return startInitiated.template(triggerComputeProviderStartedEvent(taskIdx, resubmit));
        };
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder, JobScenarioBuilder> verifyJobWithFinishedTasksCompletes() {
        return jobScenario -> {
            List<Task> activeTasks = jobScenario.getActiveTasks();
            return jobScenario.allActiveTasks(task -> assertThat(task.getStatus().getState()).isEqualTo(TaskState.Finished))
                    .advance()
                    .advance()
                    .expectJobEvent(job -> assertThat(job.getStatus().getState()).isEqualTo(JobState.Finished))
                    .expectJobUpdatedInStore(job -> assertThat(job.getStatus().getState()).isEqualTo(JobState.Finished))
                    .inAllTasks(activeTasks, jobScenario::expectedTaskArchivedInStore)
                    .advance()
                    .expectJobArchivedInStore();
        };
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder, JobScenarioBuilder> finishSingleTaskJob(
            int taskIdx, int resubmit, String reasonCode, int errorCode) {
        return jobScenario -> jobScenario
                .triggerComputePlatformFinishedEvent(taskIdx, resubmit, errorCode, reasonCode)
                .template(handleTaskFinishedTransitionInSingleTaskJob(taskIdx, resubmit, reasonCode));
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder, JobScenarioBuilder> expectTaskStateUpdate(int taskIdx, int resubmit, TaskState taskState, String reasonCode) {
        return jobScenario -> jobScenario
                .expectTaskUpdatedInStore(taskIdx, resubmit, task -> {
                    assertThat(task.getStatus().getState()).isEqualTo(taskState);
                    assertThat(task.getStatus().getReasonCode()).isEqualTo(reasonCode);
                })
                .expectTaskStateChangeEvent(taskIdx, resubmit, taskState, reasonCode);
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder, JobScenarioBuilder> handleTaskFinishedTransitionInSingleTaskJob(
            int taskIdx, int resubmit, String reasonCode) {
        return jobScenario -> jobScenario
                .expectTaskUpdatedInStore(taskIdx, resubmit, task -> {
                    assertThat(task.getStatus().getState()).isEqualTo(TaskState.Finished);
                    assertThat(task.getStatus().getReasonCode()).isEqualTo(reasonCode);
                })
                .expectTaskStateChangeEvent(taskIdx, resubmit, TaskState.Finished, reasonCode)
                .advance()
                .advance()
                .expectJobEvent(job -> assertThat(job.getStatus().getState()).isEqualTo(JobState.Finished))
                .expectJobUpdatedInStore(job -> assertThat(job.getStatus().getState()).isEqualTo(JobState.Finished))
                .advance()
                .expectedTaskArchivedInStore(taskIdx, resubmit)
                .expectJobArchivedInStore();
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder, JobScenarioBuilder> failRetryableTask(int taskIdx, int resubmit) {
        return failRetryableTask(taskIdx, resubmit, 0);
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder, JobScenarioBuilder> failRetryableTask(int taskIdx, int resubmit, long expectedRetryDelayMs) {
        return jobScenario -> jobScenario
                .triggerComputePlatformFinishedEvent(taskIdx, resubmit, -1, TaskStatus.REASON_FAILED)
                .template(cleanAfterFinishedTaskAndRetry(taskIdx, resubmit, TaskStatus.REASON_FAILED, expectedRetryDelayMs));
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder, JobScenarioBuilder> killJob() {
        return jobScenario -> jobScenario
                .killJob()
                .expectJobUpdatedInStore(job -> {
                    assertThat(job.getStatus().getState()).isEqualTo(JobState.KillInitiated);
                    assertThat(job.getStatus().getReasonCode()).isEqualTo(TaskStatus.REASON_TASK_KILLED);
                })
                .expectJobEvent(job -> assertThat(job.getStatus().getState()).isEqualTo(JobState.KillInitiated));
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder, JobScenarioBuilder> reconcilerTaskKill(int taskIdx, int resubmit) {
        return jobScenario -> jobScenario
                .expectTaskUpdatedInStore(taskIdx, resubmit, task -> assertThat(task.getStatus().getState()).isEqualTo(TaskState.KillInitiated))
                .expectTaskStateChangeEvent(taskIdx, resubmit, TaskState.KillInitiated)
                .inTask(taskIdx, resubmit, task -> jobScenario.getComputeProvider().finishTask(task.getId()))
                .expectPodTerminated(taskIdx, resubmit)
                .triggerComputePlatformFinishedEvent(taskIdx, resubmit, -1, TaskStatus.REASON_TASK_KILLED);
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder, JobScenarioBuilder> killTask(int taskIdx, int resubmit) {
        return jobScenario -> jobScenario
                .killTask(taskIdx, resubmit)
                .expectComputeProviderTaskFinished(taskIdx, resubmit)
                .expectTaskUpdatedInStore(taskIdx, resubmit, task -> {
                    assertThat(task.getStatus().getState()).isEqualTo(TaskState.KillInitiated);
                    assertThat(task.getStatus().getReasonCode()).isEqualTo(TaskStatus.REASON_TASK_KILLED);
                })
                .expectTaskStateChangeEvent(taskIdx, resubmit, TaskState.KillInitiated, TaskStatus.REASON_TASK_KILLED);
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder, JobScenarioBuilder> killKubeTask(int taskIdx, int resubmit) {
        return jobScenario -> jobScenario
                .killTask(taskIdx, resubmit)
                .inTask(taskIdx, resubmit, task -> jobScenario.getComputeProvider().finishTask(task.getId()))
                .expectPodTerminated(taskIdx, resubmit)
                .expectTaskUpdatedInStore(taskIdx, resubmit, task -> {
                    assertThat(task.getStatus().getState()).isEqualTo(TaskState.KillInitiated);
                    assertThat(task.getStatus().getReasonCode()).isEqualTo(TaskStatus.REASON_TASK_KILLED);
                })
                .expectTaskStateChangeEvent(taskIdx, resubmit, TaskState.KillInitiated, TaskStatus.REASON_TASK_KILLED);
    }

    /**
     * Batch tasks that are killed are not restarted.
     */
    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder, JobScenarioBuilder> killBatchTask(int taskIdx, int resubmit) {
        return jobScenario -> jobScenario
                .template(killTask(taskIdx, resubmit))
                .triggerComputePlatformFinishedEvent(taskIdx, resubmit, -1, TaskStatus.REASON_TASK_KILLED)
                .template(expectTaskStateUpdate(taskIdx, resubmit, TaskState.Finished, TaskStatus.REASON_TASK_KILLED));
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder, JobScenarioBuilder> passFinalKillInitiatedTimeout() {
        return jobScenario -> jobScenario
                .advance()
                .advance(JobsScenarioBuilder.KILL_INITIATED_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder, JobScenarioBuilder> passKillInitiatedTimeoutWithKillReattempt(int taskIdx, int resubmit) {
        return jobScenario -> jobScenario
                .advance()
                .advance(JobsScenarioBuilder.KILL_INITIATED_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .advance()
                .expectTaskUpdatedInStore(taskIdx, resubmit, task -> {
                    assertThat(task.getStatus().getState()).isEqualTo(TaskState.KillInitiated);
                    assertThat(task.getStatus().getReasonCode()).isEqualTo(TaskStatus.REASON_STUCK_IN_KILLING_STATE);
                })
                .expectTaskStateChangeEvent(taskIdx, resubmit, TaskState.KillInitiated, TaskStatus.REASON_STUCK_IN_KILLING_STATE)
                .expectTaskInActiveState(taskIdx, resubmit, TaskState.KillInitiated)
                .expectComputeProviderTaskFinished(taskIdx, resubmit);
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder, JobScenarioBuilder> failLastBatchRetryableTask(int taskIdx, int resubmit) {
        return jobScenario -> jobScenario
                .template(triggerComputeProviderFinishedEvent(taskIdx, resubmit, -1))
                .advance()
                .expectJobEvent(job -> assertThat(job.getStatus().getState()).isEqualTo(JobState.Finished))
                .expectJobUpdatedInStore(job -> assertThat(job.getStatus().getState()).isEqualTo(JobState.Finished))
                .advance()
                .expectedTaskArchivedInStore(taskIdx, resubmit)
                .expectJobArchivedInStore();
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder, JobScenarioBuilder> cleanAfterFinishedTaskAndRetry(int taskIdx, int resubmit, String reasonCode) {
        return cleanAfterFinishedTaskAndRetry(taskIdx, resubmit, reasonCode, 0);
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder, JobScenarioBuilder> cleanAfterFinishedTaskAndRetry(int taskIdx, int resubmit, String reasonCode, long expectedRetryDelayMs) {
        int nextResubmit = resubmit + 1;
        return jobScenario -> {
            jobScenario
                    .expectTaskUpdatedInStore(taskIdx, resubmit, task -> {
                        assertThat(task.getStatus().getState()).isEqualTo(TaskState.Finished);
                        assertThat(task.getStatus().getReasonCode()).isEqualTo(reasonCode);
                    })
                    .expectTaskStateChangeEvent(taskIdx, resubmit, TaskState.Finished, reasonCode);

            if (expectedRetryDelayMs > 0) {
                jobScenario
                        .advance(expectedRetryDelayMs / 2, TimeUnit.MILLISECONDS)
                        .expectNoStoreUpdate(taskIdx, nextResubmit)
                        .advance(expectedRetryDelayMs / 2, TimeUnit.MILLISECONDS);
            }

            return jobScenario
                    .expectTaskAddedToStore(taskIdx, nextResubmit, task -> assertThat(task.getStatus().getState()).isEqualTo(TaskState.Accepted))
                    .expectedTaskArchivedInStore(taskIdx, resubmit)
                    .expectTaskStateChangeEvent(taskIdx, nextResubmit, TaskState.Accepted)
                    .expectComputeProviderCreateRequest(taskIdx, nextResubmit);
        };
    }
}
