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

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.JobState;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import io.netflix.titus.runtime.endpoint.v3.grpc.TaskAttributes;

import static org.assertj.core.api.Assertions.assertThat;

public class ScenarioTemplates {

    public static <E extends JobDescriptor.JobDescriptorExt> Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>> acceptJobWithOneTask(int taskIdx, int resubmit) {
        return jobScenario -> jobScenario
                .expectJobEvent()
                .template(acceptTask(taskIdx, resubmit));
    }

    public static <E extends JobDescriptor.JobDescriptorExt> Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>> acceptTask(int taskIdx, int resubmit) {
        return jobScenario -> jobScenario
                .expectTaskAddedToStore(taskIdx, resubmit, task -> assertThat(task.getStatus().getState()).isEqualTo(TaskState.Accepted))
                .expectBatchTaskStateChangeEvent(taskIdx, resubmit, TaskState.Accepted)
                .expectScheduleRequest(taskIdx, resubmit);
    }

    public static <E extends JobDescriptor.JobDescriptorExt> Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>> startTask(int taskIdx, int resubmit, TaskState targetTaskState) {
        return jobScenario -> {
            JobScenarioBuilder<E> accepted = jobScenario.expectTaskInActiveState(taskIdx, resubmit, TaskState.Accepted);

            if (targetTaskState == TaskState.Accepted) {
                return accepted;
            }

            JobScenarioBuilder<E> launched = accepted
                    .triggerSchedulerLaunchEvent(taskIdx, resubmit)
                    .triggerMesosLaunchEvent(taskIdx, resubmit)
                    .expectTaskUpdatedInStore(taskIdx, resubmit, task -> {
                        assertThat(task.getStatus().getState()).isEqualTo(TaskState.Launched);
                        assertThat(task.getTwoLevelResources()).describedAs("ENI not assigned").isNotEmpty();
                    })
                    .expectBatchTaskStateChangeEvent(taskIdx, resubmit, TaskState.Launched);
            if (targetTaskState == TaskState.Launched) {
                return launched;
            }

            JobScenarioBuilder<E> startInitiated = launched
                    .triggerMesosStartInitiatedEvent(taskIdx, resubmit)
                    .expectTaskUpdatedInStore(taskIdx, resubmit, task -> assertThat(task.getStatus().getState()).isEqualTo(TaskState.StartInitiated))
                    .expectBatchTaskStateChangeEvent(taskIdx, resubmit, TaskState.StartInitiated);
            if (targetTaskState == TaskState.StartInitiated) {
                return startInitiated;
            }

            return startInitiated
                    .triggerMesosStartedEvent(taskIdx, resubmit)
                    .expectTaskUpdatedInStore(taskIdx, resubmit, task -> {
                        assertThat(task.getStatus().getState()).isEqualTo(TaskState.Started);
                        assertThat(task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP)).isNotEmpty();
                    })
                    .expectBatchTaskStateChangeEvent(taskIdx, resubmit, TaskState.Started);
        };
    }

    public static <E extends JobDescriptor.JobDescriptorExt> Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>> finishSingleTaskJob(
            int taskIdx, int resubmit, String reasonCode, int errorCode) {
        return jobScenario -> jobScenario
                .triggerMesosFinishedEvent(taskIdx, resubmit, errorCode)
                .template(handleTaskFinishedTransitionInSingleTaskJob(taskIdx, resubmit, reasonCode));
    }

    public static <E extends JobDescriptor.JobDescriptorExt> Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>> handleTaskFinishedTransitionInSingleTaskJob(
            int taskIdx, int resubmit, String reasonCode) {
        return jobScenario -> jobScenario
                .expectTaskUpdatedInStore(taskIdx, resubmit, task -> {
                    assertThat(task.getStatus().getState()).isEqualTo(TaskState.Finished);
                    assertThat(task.getStatus().getReasonCode()).isEqualTo(reasonCode);
                })
                .expectBatchTaskStateChangeEvent(taskIdx, resubmit, TaskState.Finished, reasonCode)
                .advance()
                .expectJobEvent(job -> assertThat(job.getStatus().getState()).isEqualTo(JobState.Finished))
                .expectJobUpdatedInStore(job -> assertThat(job.getStatus().getState()).isEqualTo(JobState.Finished))
                .advance()
                .expectedTaskArchivedInStore(taskIdx, resubmit)
                .expectJobArchivedInStore();
    }

    public static Function<JobScenarioBuilder<BatchJobExt>, JobScenarioBuilder<BatchJobExt>> failRetryableTask(int taskIdx, int resubmit) {
        return jobScenario -> jobScenario
                .triggerMesosFinishedEvent(taskIdx, resubmit, -1)
                .template(cleanAfterFinishedTaskAndRetry(taskIdx, resubmit));
    }

    public static Function<JobScenarioBuilder<BatchJobExt>, JobScenarioBuilder<BatchJobExt>> killTask(int taskIdx, int resubmit) {
        return jobScenario -> jobScenario
                .killTask(taskIdx, resubmit)
                .expectMesosTaskKill(0, 0)
                .expectTaskUpdatedInStore(taskIdx, resubmit, task -> {
                    assertThat(task.getStatus().getState()).isEqualTo(TaskState.KillInitiated);
                    assertThat(task.getStatus().getReasonCode()).isEqualTo(TaskStatus.REASON_TASK_KILLED);
                })
                .expectBatchTaskStateChangeEvent(taskIdx, resubmit, TaskState.KillInitiated, TaskStatus.REASON_TASK_KILLED);
    }

    public static Function<JobScenarioBuilder<BatchJobExt>, JobScenarioBuilder<BatchJobExt>> killRetryableTask(int taskIdx, int resubmit) {
        return jobScenario -> jobScenario
                .template(killTask(taskIdx, resubmit))
                .triggerMesosFinishedEvent(taskIdx, resubmit, -1)
                .template(cleanAfterFinishedTaskAndRetry(taskIdx, resubmit));
    }

    public static Function<JobScenarioBuilder<BatchJobExt>, JobScenarioBuilder<BatchJobExt>> passFinalKillInitiatedTimeout() {
        return jobScenario -> jobScenario
                .advance()
                .advance(JobsScenarioBuilder.KILL_INITIATED_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    public static Function<JobScenarioBuilder<BatchJobExt>, JobScenarioBuilder<BatchJobExt>> passKillInitiatedTimeoutWithKillReattempt(int taskIdx, int resubmit) {
        return jobScenario -> jobScenario
                .advance()
                .advance(JobsScenarioBuilder.KILL_INITIATED_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .advance()
                .expectTaskUpdatedInStore(taskIdx, resubmit, task -> {
                    assertThat(task.getStatus().getState()).isEqualTo(TaskState.KillInitiated);
                    assertThat(task.getStatus().getReasonCode()).isEqualTo(TaskStatus.REASON_STUCK_IN_STATE);
                })
                .expectBatchTaskStateChangeEvent(taskIdx, resubmit, TaskState.KillInitiated, TaskStatus.REASON_STUCK_IN_STATE)
                .expectTaskInActiveState(taskIdx, resubmit, TaskState.KillInitiated)
                .expectMesosTaskKill(taskIdx, resubmit);
    }

    public static Function<JobScenarioBuilder<BatchJobExt>, JobScenarioBuilder<BatchJobExt>> failLastRetryableTask(int taskIdx, int resubmit) {
        return jobScenario -> jobScenario
                .triggerMesosFinishedEvent(taskIdx, resubmit, -1)
                .expectTaskUpdatedInStore(taskIdx, resubmit, task -> {
                    assertThat(task.getStatus().getState()).isEqualTo(TaskState.Finished);
                    assertThat(task.getStatus().getReasonCode()).isEqualTo(TaskStatus.REASON_FAILED);
                })
                .expectBatchTaskStateChangeEvent(taskIdx, resubmit, TaskState.Finished, TaskStatus.REASON_FAILED)
                .advance()
                .expectJobEvent(job -> assertThat(job.getStatus().getState()).isEqualTo(JobState.Finished))
                .expectJobUpdatedInStore(job -> assertThat(job.getStatus().getState()).isEqualTo(JobState.Finished))
                .advance()
                .expectedTaskArchivedInStore(taskIdx, resubmit)
                .expectJobArchivedInStore();
    }

    public static Function<JobScenarioBuilder<BatchJobExt>, JobScenarioBuilder<BatchJobExt>> cleanAfterFinishedTaskAndRetry(int taskIdx, int resubmit) {
        int nextResubmit = resubmit + 1;
        return jobScenario -> jobScenario
                .expectTaskUpdatedInStore(taskIdx, resubmit, task -> {
                    assertThat(task.getStatus().getState()).isEqualTo(TaskState.Finished);
                    assertThat(task.getStatus().getReasonCode()).isEqualTo(TaskStatus.REASON_FAILED);
                })
                .expectBatchTaskStateChangeEvent(taskIdx, resubmit, TaskState.Finished, TaskStatus.REASON_FAILED)
                .advance()
                .expectTaskAddedToStore(taskIdx, nextResubmit, task -> assertThat(task.getStatus().getState()).isEqualTo(TaskState.Accepted))
                .expectedTaskArchivedInStore(taskIdx, resubmit)
                .expectBatchTaskStateChangeEvent(taskIdx, nextResubmit, TaskState.Accepted)
                .expectScheduleRequest(taskIdx, nextResubmit);
    }
}
