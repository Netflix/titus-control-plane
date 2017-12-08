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
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import io.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.api.jobmanager.model.job.retry.DelayedRetryPolicy;
import io.netflix.titus.api.jobmanager.model.job.retry.ImmediateRetryPolicy;
import io.netflix.titus.api.jobmanager.model.job.retry.RetryPolicy;
import io.netflix.titus.common.util.retry.Retryer;
import io.netflix.titus.common.util.retry.Retryers;

/**
 * Collection of functions for working with jobs and tasks.
 */
public final class JobFunctions {

    private JobFunctions() {
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

    public static boolean isServiceJob(Job<?> job) {
        return job.getJobDescriptor().getExtensions() instanceof ServiceJobExt;
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
                .withTaskContext(taskContext)
                .build();
    }

    public static BatchJobTask createNewBatchTask(Job<?> job, int index) {
        String taskId = UUID.randomUUID().toString();
        return BatchJobTask.newBuilder()
                .withId(taskId)
                .withJobId(job.getId())
                .withIndex(index)
                .withStatus(TaskStatus.newBuilder().withState(TaskState.Accepted).build())
                .withOriginalId(taskId)
                .build();
    }

    public static ServiceJobTask createNewServiceTask(Job<?> job) {
        String taskId = UUID.randomUUID().toString();
        return ServiceJobTask.newBuilder()
                .withId(taskId)
                .withJobId(job.getId())
                .withStatus(TaskStatus.newBuilder().withState(TaskState.Accepted).build())
                .withOriginalId(taskId)
                .build();
    }

    public static BatchJobTask createBatchTaskReplacement(BatchJobTask oldTask) {
        String taskId = UUID.randomUUID().toString();
        return BatchJobTask.newBuilder()
                .withId(taskId)
                .withJobId(oldTask.getJobId())
                .withIndex(oldTask.getIndex())
                .withStatus(TaskStatus.newBuilder().withState(TaskState.Accepted).build())
                .withOriginalId(oldTask.getOriginalId())
                .withResubmitOf(oldTask.getId())
                .withResubmitNumber(oldTask.getResubmitNumber() + 1)
                .build();
    }

    public static ServiceJobTask createServiceTaskReplacement(ServiceJobTask oldTask) {
        String taskId = UUID.randomUUID().toString();
        return ServiceJobTask.newBuilder()
                .withId(taskId)
                .withJobId(oldTask.getJobId())
                .withStatus(TaskStatus.newBuilder().withState(TaskState.Accepted).build())
                .withOriginalId(oldTask.getOriginalId())
                .withResubmitOf(oldTask.getId())
                .withResubmitNumber(oldTask.getResubmitNumber() + 1)
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

    public static Retryer retryerFrom(RetryPolicy retryPolicy, int remainingRetries) {
        if (remainingRetries <= 0) {
            return Retryers.never();
        }
        if (retryPolicy instanceof ImmediateRetryPolicy) {
            return Retryers.immediate(remainingRetries);
        }
        if (retryPolicy instanceof DelayedRetryPolicy) {
            return Retryers.interval(((DelayedRetryPolicy) retryPolicy).getDelayMs(), TimeUnit.MILLISECONDS, remainingRetries);
        }
        throw new IllegalArgumentException("Unknown RetryPolicy type " + retryPolicy.getClass());
    }

    public static Retryer retryer(Job<?> job, Task task) {
        RetryPolicy retryPolicy = getRetryPolicy(job);
        int remainingRetries = retryPolicy.getRetries() - task.getResubmitNumber();
        return retryerFrom(retryPolicy, remainingRetries);
    }

    public static RetryPolicy getRetryPolicy(Job<?> job) {
        JobDescriptor.JobDescriptorExt ext = job.getJobDescriptor().getExtensions();
        return ext instanceof BatchJobExt ? ((BatchJobExt) ext).getRetryPolicy() : ((ServiceJobExt) ext).getRetryPolicy();
    }

    public static JobDescriptor<BatchJobExt> changeRetryLimit(JobDescriptor<BatchJobExt> input, int retryLimit) {
        RetryPolicy newRetryPolicy = input.getExtensions().getRetryPolicy().toBuilder().withRetries(retryLimit).build();
        return input.but(jd -> input.getExtensions().toBuilder().withRetryPolicy(newRetryPolicy).build());
    }
}
