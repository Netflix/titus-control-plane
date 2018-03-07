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

package io.netflix.titus.master.jobmanager.service.integration.scenario;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import io.netflix.titus.api.jobmanager.model.job.Capacity;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor.JobDescriptorExt;
import io.netflix.titus.api.jobmanager.model.job.JobState;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.api.jobmanager.TaskAttributes;

import static org.assertj.core.api.Assertions.assertThat;

public class ScenarioTemplates {

    public static Function<JobScenarioBuilder<ServiceJobExt>, JobScenarioBuilder<ServiceJobExt>> changeJobCapacity(Capacity newCapacity) {
        return jobScenario -> jobScenario
                .changeCapacity(newCapacity)
                .expectServiceJobEvent(job -> assertThat(job.getJobDescriptor().getExtensions().getCapacity()).isEqualTo(newCapacity))
                .expectServiceJobUpdatedInStore(job -> assertThat(job.getJobDescriptor().getExtensions().getCapacity()).isEqualTo(newCapacity));
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>> acceptJobWithOneTask(int taskIdx, int resubmit) {
        return jobScenario -> jobScenario
                .expectJobEvent()
                .template(acceptTask(taskIdx, resubmit));
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>> acceptTask(int taskIdx, int resubmit) {
        return jobScenario -> jobScenario
                .expectTaskAddedToStore(taskIdx, resubmit, task -> assertThat(task.getStatus().getState()).isEqualTo(TaskState.Accepted))
                .expectTaskStateChangeEvent(taskIdx, resubmit, TaskState.Accepted)
                .expectScheduleRequest(taskIdx, resubmit);
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>> triggerMesosLaunchEvent(int taskIdx, int resubmit) {
        return jobScenario -> jobScenario
                .triggerMesosLaunchEvent(taskIdx, resubmit)
                .expectTaskUpdatedInStore(taskIdx, resubmit, task -> {
                    assertThat(task.getStatus().getState()).isEqualTo(TaskState.Launched);
                    assertThat(task.getTwoLevelResources()).describedAs("ENI not assigned").isNotEmpty();
                })
                .expectTaskStateChangeEvent(taskIdx, resubmit, TaskState.Launched);
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>> triggerMesosStartInitiatedEvent(int taskIdx, int resubmit) {
        return jobScenario -> jobScenario
                .triggerMesosStartInitiatedEvent(taskIdx, resubmit)
                .expectTaskUpdatedInStore(taskIdx, resubmit, task -> {
                    assertThat(task.getStatus().getState()).isEqualTo(TaskState.StartInitiated);
                    assertThat(task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP)).isNotEmpty();
                })
                .expectTaskStateChangeEvent(taskIdx, resubmit, TaskState.StartInitiated);
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>> triggerMesosStartedEvent(int taskIdx, int resubmit) {
        return jobScenario -> jobScenario
                .triggerMesosStartedEvent(taskIdx, resubmit)
                .expectTaskUpdatedInStore(taskIdx, resubmit, task -> assertThat(task.getStatus().getState()).isEqualTo(TaskState.Started))
                .expectTaskStateChangeEvent(taskIdx, resubmit, TaskState.Started);
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>> triggerMesosFinishedEvent(int taskIdx, int resubmit, int errorCode) {
        String reasonCode = errorCode == 0 ? TaskStatus.REASON_NORMAL : TaskStatus.REASON_FAILED;
        return jobScenario -> jobScenario
                .triggerMesosFinishedEvent(taskIdx, resubmit, errorCode, reasonCode)
                .expectTaskUpdatedInStore(taskIdx, resubmit, task -> {
                    assertThat(task.getStatus().getState()).isEqualTo(TaskState.Finished);
                    assertThat(task.getStatus().getReasonCode()).isEqualTo(reasonCode);
                })
                .expectTaskStateChangeEvent(taskIdx, resubmit, TaskState.Finished, reasonCode);

    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>> startTask(int taskIdx, int resubmit, TaskState targetTaskState) {
        return jobScenario -> {
            JobScenarioBuilder<E> accepted = jobScenario.expectTaskInActiveState(taskIdx, resubmit, TaskState.Accepted);
            if (targetTaskState == TaskState.Accepted) {
                return accepted;
            }

            JobScenarioBuilder<E> launched = accepted.triggerSchedulerLaunchEvent(taskIdx, resubmit).template(triggerMesosLaunchEvent(taskIdx, resubmit));
            if (targetTaskState == TaskState.Launched) {
                return launched;
            }

            JobScenarioBuilder<E> startInitiated = launched.template(triggerMesosStartInitiatedEvent(taskIdx, resubmit));
            if (targetTaskState == TaskState.StartInitiated) {
                return startInitiated;
            }

            return startInitiated.template(triggerMesosStartedEvent(taskIdx, resubmit));
        };
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>> verifyJobWithFinishedTasksCompletes() {
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

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>> finishSingleTaskJob(
            int taskIdx, int resubmit, String reasonCode, int errorCode) {
        return jobScenario -> jobScenario
                .triggerMesosFinishedEvent(taskIdx, resubmit, errorCode, reasonCode)
                .template(handleTaskFinishedTransitionInSingleTaskJob(taskIdx, resubmit, reasonCode));
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>> expectTaskStateUpdate(int taskIdx, int resubmit, TaskState taskState, String reasonCode) {
        return jobScenario -> jobScenario
                .expectTaskUpdatedInStore(taskIdx, resubmit, task -> {
                    assertThat(task.getStatus().getState()).isEqualTo(taskState);
                    assertThat(task.getStatus().getReasonCode()).isEqualTo(reasonCode);
                })
                .expectTaskStateChangeEvent(taskIdx, resubmit, taskState, reasonCode);
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>> handleTaskFinishedTransitionInSingleTaskJob(
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

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>> failRetryableTask(int taskIdx, int resubmit) {
        return failRetryableTask(taskIdx, resubmit, 0);
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>> failRetryableTask(int taskIdx, int resubmit, long expectedRetryDelayMs) {
        return jobScenario -> jobScenario
                .triggerMesosFinishedEvent(taskIdx, resubmit, -1, TaskStatus.REASON_FAILED)
                .template(cleanAfterFinishedTaskAndRetry(taskIdx, resubmit, TaskStatus.REASON_FAILED, expectedRetryDelayMs));
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>> killJob() {
        return jobScenario -> jobScenario
                .killJob()
                .expectJobUpdatedInStore(job -> {
                    assertThat(job.getStatus().getState()).isEqualTo(JobState.KillInitiated);
                    assertThat(job.getStatus().getReasonCode()).isEqualTo(TaskStatus.REASON_TASK_KILLED);
                })
                .expectJobEvent(job -> assertThat(job.getStatus().getState()).isEqualTo(JobState.KillInitiated));
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>> reconcilerTaskKill(int taskIdx, int resubmit) {
        return jobScenario -> jobScenario
                .expectMesosTaskKill(taskIdx, resubmit)
                .expectTaskUpdatedInStore(taskIdx, resubmit, task -> assertThat(task.getStatus().getState()).isEqualTo(TaskState.KillInitiated))
                .expectTaskStateChangeEvent(taskIdx, resubmit, TaskState.KillInitiated)
                .triggerMesosFinishedEvent(taskIdx, resubmit, -1, TaskStatus.REASON_TASK_KILLED);
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>> killTask(int taskIdx, int resubmit) {
        return jobScenario -> jobScenario
                .killTask(taskIdx, resubmit)
                .expectMesosTaskKill(taskIdx, resubmit)
                .expectTaskUpdatedInStore(taskIdx, resubmit, task -> {
                    assertThat(task.getStatus().getState()).isEqualTo(TaskState.KillInitiated);
                    assertThat(task.getStatus().getReasonCode()).isEqualTo(TaskStatus.REASON_TASK_KILLED);
                })
                .expectTaskStateChangeEvent(taskIdx, resubmit, TaskState.KillInitiated, TaskStatus.REASON_TASK_KILLED);
    }

    /**
     * Batch tasks that are killed are not restarted.
     */
    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>> killBatchTask(int taskIdx, int resubmit) {
        return jobScenario -> jobScenario
                .template(killTask(taskIdx, resubmit))
                .triggerMesosFinishedEvent(taskIdx, resubmit, -1, TaskStatus.REASON_TASK_KILLED)
                .template(expectTaskStateUpdate(taskIdx, resubmit, TaskState.Finished, TaskStatus.REASON_TASK_KILLED));
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>> passFinalKillInitiatedTimeout() {
        return jobScenario -> jobScenario
                .advance()
                .advance(JobsScenarioBuilder.KILL_INITIATED_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>> passKillInitiatedTimeoutWithKillReattempt(int taskIdx, int resubmit) {
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
                .expectMesosTaskKill(taskIdx, resubmit);
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>> failLastBatchRetryableTask(int taskIdx, int resubmit) {
        return jobScenario -> jobScenario
                .template(triggerMesosFinishedEvent(taskIdx, resubmit, -1))
                .advance()
                .expectJobEvent(job -> assertThat(job.getStatus().getState()).isEqualTo(JobState.Finished))
                .expectJobUpdatedInStore(job -> assertThat(job.getStatus().getState()).isEqualTo(JobState.Finished))
                .advance()
                .expectedTaskArchivedInStore(taskIdx, resubmit)
                .expectJobArchivedInStore();
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>> cleanAfterFinishedTaskAndRetry(int taskIdx, int resubmit, String reasonCode) {
        return cleanAfterFinishedTaskAndRetry(taskIdx, resubmit, reasonCode, 0);
    }

    public static <E extends JobDescriptorExt> Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>> cleanAfterFinishedTaskAndRetry(int taskIdx, int resubmit, String reasonCode, long expectedRetryDelayMs) {
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
                    .expectScheduleRequest(taskIdx, nextResubmit);
        };
    }
}
