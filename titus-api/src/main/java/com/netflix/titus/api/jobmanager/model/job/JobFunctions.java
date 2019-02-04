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

package com.netflix.titus.api.jobmanager.model.job;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor.JobDescriptorExt;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.ContainerHealthProvider;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudget;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.SelfManagedDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.UnlimitedDisruptionBudgetRate;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.model.job.retry.DelayedRetryPolicy;
import com.netflix.titus.api.jobmanager.model.job.retry.ExponentialBackoffRetryPolicy;
import com.netflix.titus.api.jobmanager.model.job.retry.ImmediateRetryPolicy;
import com.netflix.titus.api.jobmanager.model.job.retry.RetryPolicy;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.api.model.v2.V2JobState;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.retry.Retryer;
import com.netflix.titus.common.util.retry.Retryers;
import com.netflix.titus.common.util.time.Clock;

/**
 * Collection of functions for working with jobs and tasks.
 */
public final class JobFunctions {

    private static final DisruptionBudget NO_DISRUPTION_BUDGET_MARKER = DisruptionBudget.newBuilder()
            .withDisruptionBudgetPolicy(SelfManagedDisruptionBudgetPolicy.newBuilder().build())
            .withDisruptionBudgetRate(UnlimitedDisruptionBudgetRate.newBuilder().build())
            .withContainerHealthProviders(Collections.emptyList())
            .withTimeWindows(Collections.emptyList())
            .build();

    private JobFunctions() {
    }

    public static DisruptionBudget getNoDisruptionBudgetMarker() {
        return NO_DISRUPTION_BUDGET_MARKER;
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

    public static boolean isBatchJob(Job<?> job) {
        return job.getJobDescriptor().getExtensions() instanceof BatchJobExt;
    }

    public static boolean isBatchJob(JobDescriptor<?> jobDescriptor) {
        return jobDescriptor.getExtensions() instanceof BatchJobExt;
    }

    public static Job<BatchJobExt> asBatchJob(Job<?> job) {
        if (isBatchJob(job)) {
            return (Job<BatchJobExt>) job;
        }
        throw JobManagerException.notBatchJob(job.getId());
    }

    public static JobDescriptor<BatchJobExt> asBatchJob(JobDescriptor<?> jobDescriptor) {
        if (isBatchJob(jobDescriptor)) {
            return (JobDescriptor<BatchJobExt>) jobDescriptor;
        }
        throw JobManagerException.notBatchJobDescriptor(jobDescriptor);
    }

    public static boolean isServiceJob(Job<?> job) {
        return job.getJobDescriptor().getExtensions() instanceof ServiceJobExt;
    }

    public static boolean isServiceJob(JobDescriptor<?> jobDescriptor) {
        return jobDescriptor.getExtensions() instanceof ServiceJobExt;
    }

    public static Job<ServiceJobExt> asServiceJob(Job<?> job) {
        if (isServiceJob(job)) {
            return (Job<ServiceJobExt>) job;
        }
        throw JobManagerException.notServiceJob(job.getId());
    }

    public static JobDescriptor<ServiceJobExt> asServiceJob(JobDescriptor<?> jobDescriptor) {
        if (isServiceJob(jobDescriptor)) {
            return (JobDescriptor<ServiceJobExt>) jobDescriptor;
        }
        throw JobManagerException.notServiceJobDescriptor(jobDescriptor);
    }

    public static int getJobDesiredSize(Job<?> job) {
        return isServiceJob(job)
                ? ((ServiceJobExt) job.getJobDescriptor().getExtensions()).getCapacity().getDesired()
                : ((BatchJobExt) job.getJobDescriptor().getExtensions()).getSize();
    }

    public static boolean isBatchTask(Task task) {
        return task instanceof BatchJobTask;
    }

    public static boolean isServiceTask(Task task) {
        return task instanceof ServiceJobTask;
    }

    public static boolean hasDisruptionBudget(JobDescriptor<?> jobDescriptor) {
        return !NO_DISRUPTION_BUDGET_MARKER.equals(jobDescriptor.getDisruptionBudget());
    }

    public static boolean hasDisruptionBudget(Job<?> job) {
        return !NO_DISRUPTION_BUDGET_MARKER.equals(job.getJobDescriptor().getDisruptionBudget());
    }

    /**
     * @return true if the task is or was at some point in time in the started state.
     */
    public static boolean everStarted(Task task) {
        return findTaskStatus(task, TaskState.Started).isPresent();
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

    public static <E extends JobDescriptorExt> JobDescriptor<E> appendJobDescriptorAttribute(JobDescriptor<E> jobDescriptor,
                                                                                             String attributeName,
                                                                                             Object attributeValue) {
        return jobDescriptor.toBuilder()
                .withAttributes(CollectionsExt.copyAndAdd(jobDescriptor.getAttributes(), attributeName, "" + attributeValue))
                .build();
    }

    public static <E extends JobDescriptorExt> Job<E> appendJobDescriptorAttribute(Job<E> job,
                                                                                   String attributeName,
                                                                                   Object attributeValue) {
        return job.toBuilder()
                .withJobDescriptor(appendJobDescriptorAttribute(job.getJobDescriptor(), attributeName, attributeValue))
                .build();
    }

    public static Task appendTaskAttribute(Task task, String attributeName, Object attributeValue) {
        return task.toBuilder()
                .withAttributes(CollectionsExt.copyAndAdd(task.getAttributes(), attributeName, "" + attributeValue))
                .build();
    }

    public static Job<BatchJobExt> changeBatchJobSize(Job<BatchJobExt> job, int size) {
        return job.toBuilder().withJobDescriptor(changeBatchJobSize(job.getJobDescriptor(), size)).build();
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

    public static <E extends JobDescriptorExt> JobDescriptor<E> incrementJobDescriptorSize(JobDescriptor<E> jobDescriptor, int delta) {
        if (isServiceJob(jobDescriptor)) {
            Capacity oldCapacity = ((ServiceJobExt) jobDescriptor.getExtensions()).getCapacity();
            Capacity newCapacity = oldCapacity.toBuilder().withDesired(oldCapacity.getDesired() + delta).build();
            return (JobDescriptor<E>) changeServiceJobCapacity((JobDescriptor<ServiceJobExt>) jobDescriptor, newCapacity);
        }

        JobDescriptor<BatchJobExt> batchDescriptor = (JobDescriptor<BatchJobExt>) jobDescriptor;
        return (JobDescriptor<E>) changeBatchJobSize(batchDescriptor, batchDescriptor.getExtensions().getSize() + delta);
    }

    public static <E extends JobDescriptorExt> Job<E> incrementJobSize(Job<E> job, int delta) {
        return job.toBuilder().withJobDescriptor(incrementJobDescriptorSize(job.getJobDescriptor(), delta)).build();
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

    public static Task fixArchivedTaskStatus(Task task, Clock clock) {
        Task fixed = task.toBuilder()
                .withStatus(TaskStatus.newBuilder()
                        .withState(TaskState.Finished)
                        .withReasonCode("inconsistent")
                        .withReasonMessage("Expecting task in Finished state, but is " + task.getStatus().getState())
                        .withTimestamp(clock.wallTime())
                        .build()
                )
                .withStatusHistory(CollectionsExt.copyAndAdd(task.getStatusHistory(), task.getStatus()))
                .build();
        return fixed;
    }

    public static Task addAllocatedResourcesToTask(Task task, TaskStatus status, TwoLevelResource twoLevelResource, Map<String, String> taskContext) {
        return taskStatusChangeBuilder(task, status)
                .withTwoLevelResources(twoLevelResource)
                .addAllToTaskContext(taskContext)
                .build();
    }

    public static <E extends JobDescriptorExt> Function<Job<E>, Job<E>> withJobId(String jobId) {
        return job -> job.toBuilder().withId(jobId).build();
    }

    public static Function<JobDescriptor<BatchJobExt>, JobDescriptor<BatchJobExt>> ofBatchSize(int size) {
        return jd -> JobFunctions.changeBatchJobSize(jd, size);
    }

    public static Function<JobDescriptor<ServiceJobExt>, JobDescriptor<ServiceJobExt>> ofServiceSize(int size) {
        return jd -> JobFunctions.changeServiceJobCapacity(jd, size);
    }

    public static <E extends JobDescriptorExt> Function<JobDescriptor<E>, JobDescriptor<E>> havingProvider(String name, String... attributes) {
        ContainerHealthProvider newProvider = ContainerHealthProvider.newBuilder()
                .withName(name)
                .withAttributes(CollectionsExt.asMap(attributes))
                .build();

        return jd -> {
            List<ContainerHealthProvider> existing = jd.getDisruptionBudget().getContainerHealthProviders().stream()
                    .filter(p -> !p.getName().equals(name))
                    .collect(Collectors.toList());

            List<ContainerHealthProvider> providers = new ArrayList<>(existing);
            providers.add(newProvider);

            return jd.toBuilder()
                    .withDisruptionBudget(
                            jd.getDisruptionBudget().toBuilder()
                                    .withContainerHealthProviders(providers)
                                    .build()
                    ).build();
        };
    }

    public static <E extends JobDescriptorExt> Function<JobDescriptor<E>, JobDescriptor<E>> withDisruptionBudget(DisruptionBudget disruptionBudget) {
        return jobDescriptor -> jobDescriptor.toBuilder().withDisruptionBudget(disruptionBudget).build();
    }

    public static <E extends JobDescriptorExt> Function<Job<E>, Job<E>> withJobDisruptionBudget(DisruptionBudget disruptionBudget) {
        return job -> job.toBuilder()
                .withJobDescriptor(job.getJobDescriptor().toBuilder().withDisruptionBudget(disruptionBudget).build())
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

    public static <E extends JobDescriptorExt> JobDescriptor<E> changeDisruptionBudget(JobDescriptor<E> input, DisruptionBudget disruptionBudget) {
        return input.toBuilder().withDisruptionBudget(disruptionBudget).build();
    }

    public static <E extends JobDescriptorExt> Job<E> changeDisruptionBudget(Job<E> input, DisruptionBudget disruptionBudget) {
        return input.toBuilder().withJobDescriptor(changeDisruptionBudget(input.getJobDescriptor(), disruptionBudget)).build();
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

    public static Task moveTask(String jobIdFrom, String jobIdTo, Task taskBefore) {
        return taskBefore.toBuilder()
                .withJobId(jobIdTo)
                .addToTaskContext(TaskAttributes.TASK_ATTRIBUTES_MOVED_FROM_JOB, jobIdFrom)
                .build();
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

    public static Optional<JobStatus> findJobStatus(Job<?> job, JobState checkedState) {
        if (job.getStatus().getState() == checkedState) {
            return Optional.of(job.getStatus());
        }
        for (JobStatus jobStatus : job.getStatusHistory()) {
            if (jobStatus.getState() == checkedState) {
                return Optional.of(jobStatus);
            }
        }
        return Optional.empty();
    }

    public static Optional<TaskStatus> findTaskStatus(Task task, TaskState checkedState) {
        if (task.getStatus().getState() == checkedState) {
            return Optional.of(task.getStatus());
        }
        for (TaskStatus taskStatus : task.getStatusHistory()) {
            if (taskStatus.getState() == checkedState) {
                return Optional.of(taskStatus);
            }
        }
        return Optional.empty();
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
