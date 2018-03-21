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

package io.netflix.titus.api.jobmanager.model.job;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.netflix.titus.api.jobmanager.model.job.JobDescriptor.JobDescriptorExt;
import io.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.api.jobmanager.model.job.retry.DelayedRetryPolicy;
import io.netflix.titus.api.jobmanager.model.job.retry.ExponentialBackoffRetryPolicy;
import io.netflix.titus.api.jobmanager.model.job.retry.ImmediateRetryPolicy;
import io.netflix.titus.api.jobmanager.model.job.retry.RetryPolicy;
import io.netflix.titus.api.model.v2.V2JobState;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.common.util.retry.Retryer;
import io.netflix.titus.common.util.retry.Retryers;
import io.netflix.titus.common.util.time.Clock;

/**
 * Collection of functions for working with jobs and tasks.
 */
public final class JobFunctions {

    private JobFunctions() {
    }

    @Deprecated
    public static V2JobState toV2JobState(TaskState v3TaskState) {
        switch (v3TaskState) {
            case Accepted:
                return V2JobState.Accepted;
            case Launched:
                return V2JobState.Launched;
            case StartInitiated:
                return V2JobState.StartInitiated;
            case Started:
            case KillInitiated:
                return V2JobState.Started;
            case Finished:
                return V2JobState.Completed;
        }
        throw new IllegalStateException("Unexpected V3 task state: " + v3TaskState);
    }

    public static boolean isV2JobId(String jobId) {
        return jobId.startsWith("Titus-");
    }

    public static boolean isV2Task(String taskId) {
        return isV2JobId(taskId);
    }

    public static boolean isBatchJob(Job<?> job) {
        return job.getJobDescriptor().getExtensions() instanceof BatchJobExt;
    }

    public static boolean isBatchJob(JobDescriptor<?> jobDescriptor) {
        return jobDescriptor.getExtensions() instanceof BatchJobExt;
    }

    public static boolean isServiceJob(Job<?> job) {
        return job.getJobDescriptor().getExtensions() instanceof ServiceJobExt;
    }

    public static boolean isServiceJob(JobDescriptor<?> jobDescriptor) {
        return jobDescriptor.getExtensions() instanceof ServiceJobExt;
    }

    public static boolean isBatchTask(Task task) {
        return task instanceof BatchJobTask;
    }

    public static boolean isServiceTask(Task task) {
        return task instanceof ServiceJobTask;
    }

    public static Job changeJobStatus(Job job, JobState jobState, String reasonCode) {
        JobStatus newStatus = JobModel.newJobStatus()
                .withState(jobState)
                .withReasonCode(reasonCode)
                .build();
        return JobFunctions.changeJobStatus(job, newStatus);
    }

    public static Job changeJobStatus(Job job, JobStatus status) {
        JobStatus currentStatus = job.getStatus();
        List<JobStatus> statusHistory = new ArrayList<>(job.getStatusHistory());
        statusHistory.add(currentStatus);
        return job.toBuilder()
                .withStatus(status)
                .withStatusHistory(statusHistory)
                .build();
    }

    public static JobDescriptor<BatchJobExt> changeBatchJobSize(JobDescriptor<BatchJobExt> jobDescriptor, int size) {
        BatchJobExt ext = jobDescriptor.getExtensions().toBuilder().withSize(size).build();
        return jobDescriptor.toBuilder().withExtensions(ext).build();
    }

    public static JobDescriptor<ServiceJobExt> changeServiceJobCapacity(JobDescriptor<ServiceJobExt> jobDescriptor, int size) {
        return changeServiceJobCapacity(jobDescriptor, Capacity.newBuilder().withMin(size).withDesired(size).withMax(size).build());
    }

    public static JobDescriptor<ServiceJobExt> changeServiceJobCapacity(JobDescriptor<ServiceJobExt> jobDescriptor, Capacity capacity) {
        return jobDescriptor.toBuilder()
                .withExtensions(jobDescriptor.getExtensions().toBuilder()
                        .withCapacity(capacity)
                        .build()
                )
                .build();
    }

    public static Job<ServiceJobExt> changeServiceJobCapacity(Job<ServiceJobExt> job, Capacity capacity) {
        return job.toBuilder().withJobDescriptor(changeServiceJobCapacity(job.getJobDescriptor(), capacity)).build();
    }

    public static Job<ServiceJobExt> changeJobEnabledStatus(Job<ServiceJobExt> job, boolean enabled) {
        JobDescriptor<ServiceJobExt> jobDescriptor = job.getJobDescriptor().toBuilder()
                .withExtensions(job.getJobDescriptor().getExtensions().toBuilder()
                        .withEnabled(enabled)
                        .build()
                )
                .build();
        return job.toBuilder().withJobDescriptor(jobDescriptor).build();
    }

    public static Job<ServiceJobExt> changeServiceJobProcesses(Job<ServiceJobExt> job, ServiceJobProcesses processes) {
        return job.toBuilder().withJobDescriptor(job.getJobDescriptor().but(jd ->
                jd.getExtensions().toBuilder().withServiceJobProcesses(processes).build()
        )).build();
    }

    public static Task changeTaskStatus(Task task, TaskStatus status) {
        return taskStatusChangeBuilder(task, status).build();
    }

    public static Task changeTaskStatus(Task task, TaskState taskState, String reasonCode, String reasonMessage) {
        TaskStatus newStatus = JobModel.newTaskStatus()
                .withState(taskState)
                .withReasonCode(reasonCode)
                .withReasonMessage(reasonMessage)
                .build();
        return taskStatusChangeBuilder(task, newStatus).build();
    }

    public static Task addAllocatedResourcesToTask(Task task, TaskStatus status, TwoLevelResource twoLevelResource, Map<String, String> taskContext) {
        return taskStatusChangeBuilder(task, status)
                .withTwoLevelResources(twoLevelResource)
                .addAllToTaskContext(taskContext)
                .build();
    }

    private static Task.TaskBuilder taskStatusChangeBuilder(Task task, TaskStatus status) {
        TaskStatus currentStatus = task.getStatus();
        List<TaskStatus> statusHistory = new ArrayList<>(task.getStatusHistory());
        statusHistory.add(currentStatus);
        return task.toBuilder()
                .withStatus(status)
                .withStatusHistory(statusHistory);
    }

    public static Retryer retryerFrom(RetryPolicy retryPolicy) {
        if (retryPolicy instanceof ImmediateRetryPolicy) {
            return Retryers.immediate();
        }
        if (retryPolicy instanceof DelayedRetryPolicy) {
            return Retryers.interval(((DelayedRetryPolicy) retryPolicy).getDelayMs(), TimeUnit.MILLISECONDS);
        }
        if (retryPolicy instanceof ExponentialBackoffRetryPolicy) {
            ExponentialBackoffRetryPolicy exponential = (ExponentialBackoffRetryPolicy) retryPolicy;
            return Retryers.exponentialBackoff(exponential.getInitialDelayMs(), exponential.getMaxDelayMs(), TimeUnit.MILLISECONDS);
        }
        throw new IllegalArgumentException("Unknown RetryPolicy type " + retryPolicy.getClass());
    }

    public static Retryer retryer(Job<?> job) {
        RetryPolicy retryPolicy = getRetryPolicy(job);
        return retryerFrom(retryPolicy);
    }

    public static RetryPolicy getRetryPolicy(Job<?> job) {
        JobDescriptorExt ext = job.getJobDescriptor().getExtensions();
        return ext instanceof BatchJobExt ? ((BatchJobExt) ext).getRetryPolicy() : ((ServiceJobExt) ext).getRetryPolicy();
    }

    public static <E extends JobDescriptorExt> JobDescriptor<E> changeRetryPolicy(JobDescriptor<E> input, RetryPolicy retryPolicy) {
        if (input.getExtensions() instanceof BatchJobExt) {
            JobDescriptor<BatchJobExt> batchJob = (JobDescriptor<BatchJobExt>) input;
            return (JobDescriptor<E>) batchJob.but(jd -> batchJob.getExtensions().toBuilder().withRetryPolicy(retryPolicy).build());
        }
        JobDescriptor<ServiceJobExt> serviceJob = (JobDescriptor<ServiceJobExt>) input;
        return (JobDescriptor<E>) serviceJob.but(jd -> serviceJob.getExtensions().toBuilder().withRetryPolicy(retryPolicy).build());
    }

    public static JobDescriptor<BatchJobExt> changeRetryLimit(JobDescriptor<BatchJobExt> input, int retryLimit) {
        RetryPolicy newRetryPolicy = input.getExtensions().getRetryPolicy().toBuilder().withRetries(retryLimit).build();
        return input.but(jd -> input.getExtensions().toBuilder().withRetryPolicy(newRetryPolicy).build());
    }

    public static Optional<Long> getTimeInState(Task task, TaskState checkedState, Clock clock) {
        return findTaskStatus(task, checkedState).map(checkedStatus -> {
            TaskState currentState = task.getStatus().getState();
            if (currentState == checkedState) {
                return clock.wallTime() - task.getStatus().getTimestamp();
            }
            if (TaskState.isAfter(checkedState, currentState)) {
                return 0L;
            }
            return findStatusAfter(task, checkedState).map(after -> after.getTimestamp() - checkedStatus.getTimestamp()).orElse(0L);
        });
    }

    /**
     * Check that the given task transitioned through the expected states. Duplicates of a state are collapsed into single state.
     */
    public static boolean containsExactlyTaskStates(Task task, TaskState... expectedStates) {
        if (expectedStates.length == 0) {
            return false;
        }

        TaskState taskState = task.getStatus().getState();
        List<TaskStatus> statusHistory = task.getStatusHistory();
        if (expectedStates.length == 1 && statusHistory.isEmpty()) {
            return taskState == expectedStates[0];
        }

        // For non-single state values, we have to eliminate possible duplicates.
        Set<TaskState> expectedPreviousStates = CollectionsExt.asSet(expectedStates);
        Set<TaskState> taskStates = statusHistory.stream().map(ExecutableStatus::getState).collect(Collectors.toSet());
        taskStates.add(taskState);

        return expectedPreviousStates.equals(taskStates);
    }

    public static Optional<TaskStatus> findTaskStatus(Task task, TaskState checkedState) {
        if (task.getStatus().getState() == checkedState) {
            return Optional.of(task.getStatus());
        }
        return task.getStatusHistory().stream().filter(s -> s.getState() == checkedState).findFirst();
    }

    public static Optional<TaskStatus> findStatusAfter(Task task, TaskState before) {
        TaskStatus after = null;
        for (TaskStatus status : task.getStatusHistory()) {
            if (TaskState.isAfter(status.getState(), before)) {
                if (after == null) {
                    after = status;
                } else if (TaskState.isAfter(after.getState(), status.getState())) {
                    after = status;
                }
            }
        }
        if (after == null && TaskState.isAfter(task.getStatus().getState(), before)) {
            after = task.getStatus();
        }
        return Optional.ofNullable(after);
    }
}
