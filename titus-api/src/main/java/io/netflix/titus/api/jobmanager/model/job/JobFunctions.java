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
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import io.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
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

    public static void checkBatchIndexInvariant(Job<BatchJobExt> job, int index) {
        int size = job.getJobDescriptor().getExtensions().getSize();
        Preconditions.checkArgument(index >= 0, "Batch task index must be >= 0");
        Preconditions.checkArgument(index < size, "Batch task index must be < job size(%s)", size);
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

    public static Task updateTaskStatus(Task task, TaskStatus status) {
        return taskStatusChangeBuilder(task, status).build();
    }

    public static Task updateTaskStatus(Task task, TaskState taskState, String reasonCode, String reasonMessage) {
        TaskStatus newStatus = JobModel.newTaskStatus()
                .withState(taskState)
                .withReasonCode(reasonCode)
                .withReasonMessage(reasonMessage)
                .build();
        return taskStatusChangeBuilder(task, newStatus).build();
    }

    public static Task updateTaskAfterScheduling(Task task, TaskStatus status, TwoLevelResource twoLevelResource, Map<String, String> taskContext) {
        return taskStatusChangeBuilder(task, status)
                .withTwoLevelResources(twoLevelResource)
                .withTaskContext(taskContext)
                .build();
    }

    public static Job updateJobStatus(Job job, JobStatus status) {
        JobStatus currentStatus = job.getStatus();
        List<JobStatus> statusHistory = new ArrayList<>(job.getStatusHistory());
        statusHistory.add(currentStatus);
        return job.toBuilder()
                .withStatus(status)
                .withStatusHistory(statusHistory)
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
}
