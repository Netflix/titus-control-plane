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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor.JobDescriptorExt;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.ContainerHealthProvider;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudget;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.SelfManagedDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.UnlimitedDisruptionBudgetRate;
import com.netflix.titus.api.jobmanager.model.job.ebs.EbsVolume;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.model.job.retry.DelayedRetryPolicy;
import com.netflix.titus.api.jobmanager.model.job.retry.ExponentialBackoffRetryPolicy;
import com.netflix.titus.api.jobmanager.model.job.retry.ImmediateRetryPolicy;
import com.netflix.titus.api.jobmanager.model.job.retry.RetryPolicy;
import com.netflix.titus.api.jobmanager.model.job.vpc.SignedIpAddressAllocation;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.retry.Retryer;
import com.netflix.titus.common.util.retry.Retryers;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.common.util.tuple.Pair;

/**
 * Collection of functions for working with jobs and tasks.
 */
public final class JobFunctions {

    private static final String DEFAULT_APPLICATION = "DEFAULT";

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

    public static JobType getJobType(Job<?> job) {
        return isServiceJob(job) ? JobType.SERVICE : JobType.BATCH;
    }

    public static JobType getJobType(JobDescriptor<?> jobDescriptor) {
        return isServiceJob(jobDescriptor) ? JobType.SERVICE : JobType.BATCH;
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

    public static boolean isDisabled(Job<?> job) {
        return JobFunctions.isServiceJob(job) && !((ServiceJobExt) job.getJobDescriptor().getExtensions()).isEnabled();
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

    public static JobDescriptor<?> filterOutGeneratedAttributes(JobDescriptor<?> jobDescriptor) {
        return jobDescriptor.toBuilder().withAttributes(
                CollectionsExt.copyAndRemoveByKey(jobDescriptor.getAttributes(),
                        key -> key.startsWith(JobAttributes.JOB_ATTRIBUTE_SANITIZATION_PREFIX) ||
                                key.startsWith(JobAttributes.PREDICTION_ATTRIBUTE_PREFIX)
                )
        ).build();
    }

    public static <E extends JobDescriptorExt> JobDescriptor<E> appendJobDescriptorAttributes(JobDescriptor<E> jobDescriptor,
                                                                                              Map<String, ?> attributes) {
        Map<String, String> asStrings = attributes.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
        return jobDescriptor.toBuilder()
                .withAttributes(CollectionsExt.copyAndAdd(jobDescriptor.getAttributes(), asStrings))
                .build();
    }

    public static <E extends JobDescriptorExt> JobDescriptor<E> appendJobDescriptorAttribute(JobDescriptor<E> jobDescriptor,
                                                                                             String attributeName,
                                                                                             Object attributeValue) {
        return jobDescriptor.toBuilder()
                .withAttributes(CollectionsExt.copyAndAdd(jobDescriptor.getAttributes(), attributeName, "" + attributeValue))
                .build();
    }

    public static <E extends JobDescriptorExt> JobDescriptor<E> appendContainerAttribute(JobDescriptor<E> jobDescriptor,
                                                                                         String attributeName,
                                                                                         Object attributeValue) {
        return jobDescriptor.toBuilder()
                .withContainer(jobDescriptor.getContainer().toBuilder()
                        .withAttributes(CollectionsExt.copyAndAdd(
                                jobDescriptor.getContainer().getAttributes(), attributeName, "" + attributeValue))
                        .build()
                )
                .build();
    }

    public static <E extends JobDescriptorExt> Job<E> appendContainerAttribute(Job<E> job,
                                                                               String attributeName,
                                                                               Object attributeValue) {
        return job.toBuilder()
                .withJobDescriptor(appendContainerAttribute(job.getJobDescriptor(), attributeName, attributeValue))
                .build();
    }

    public static <E extends JobDescriptorExt> JobDescriptor<E> appendHardConstraint(JobDescriptor<E> jobDescriptor,
                                                                                     String name,
                                                                                     String value) {
        return jobDescriptor.toBuilder()
                .withContainer(jobDescriptor.getContainer().toBuilder()
                        .withHardConstraints(CollectionsExt.copyAndAdd(jobDescriptor.getContainer().getHardConstraints(), name, value))
                        .build()
                )
                .build();
    }

    public static <E extends JobDescriptorExt> JobDescriptor<E> appendSoftConstraint(JobDescriptor<E> jobDescriptor,
                                                                                     String name,
                                                                                     String value) {
        return jobDescriptor.toBuilder()
                .withContainer(jobDescriptor.getContainer().toBuilder()
                        .withSoftConstraints(CollectionsExt.copyAndAdd(jobDescriptor.getContainer().getSoftConstraints(), name, value))
                        .build()
                )
                .build();
    }

    public static <E extends JobDescriptorExt> Job<E> appendHardConstraint(Job<E> job, String name, String value) {
        return job.toBuilder().withJobDescriptor(appendHardConstraint(job.getJobDescriptor(), name, value)).build();
    }

    public static <E extends JobDescriptorExt> Job<E> appendSoftConstraint(Job<E> job, String name, String value) {
        return job.toBuilder().withJobDescriptor(appendSoftConstraint(job.getJobDescriptor(), name, value)).build();
    }

    public static <E extends JobDescriptorExt> Job<E> appendJobDescriptorAttribute(Job<E> job,
                                                                                   String attributeName,
                                                                                   Object attributeValue) {
        return job.toBuilder()
                .withJobDescriptor(appendJobDescriptorAttribute(job.getJobDescriptor(), attributeName, attributeValue))
                .build();
    }

    /**
     * Constraint names are case insensitive.
     */
    public static <E extends JobDescriptorExt> Optional<String> findHardConstraint(Job<E> job, String name) {
        return findConstraint(job.getJobDescriptor().getContainer().getHardConstraints(), name);
    }

    public static <E extends JobDescriptorExt> Optional<String> findHardConstraint(JobDescriptor<E> jobDescriptor, String name) {
        return findConstraint(jobDescriptor.getContainer().getHardConstraints(), name);
    }

    /**
     * Constraint names are case insensitive.
     */
    public static <E extends JobDescriptorExt> Optional<String> findSoftConstraint(Job<E> job, String name) {
        return findConstraint(job.getJobDescriptor().getContainer().getSoftConstraints(), name);
    }

    private static Optional<String> findConstraint(Map<String, String> constraints, String name) {
        if (CollectionsExt.isNullOrEmpty(constraints)) {
            return Optional.empty();
        }
        for (String key : constraints.keySet()) {
            if (key.equalsIgnoreCase(name)) {
                return Optional.ofNullable(constraints.get(key));
            }
        }
        return Optional.empty();
    }

    public static Task appendTaskAttribute(Task task, String attributeName, Object attributeValue) {
        return task.toBuilder()
                .withAttributes(CollectionsExt.copyAndAdd(task.getAttributes(), attributeName, "" + attributeValue))
                .build();
    }

    public static Task appendTaskContext(Task task, String contextName, Object contextValue) {
        return task.toBuilder()
                .withTaskContext(CollectionsExt.copyAndAdd(task.getTaskContext(), contextName, "" + contextValue))
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

    public static Job<ServiceJobExt> changeServiceJobCapacity(Job<ServiceJobExt> job, CapacityAttributes capacityAttributes) {
        Capacity.Builder newCapacityBuilder = job.getJobDescriptor().getExtensions().getCapacity().toBuilder();
        capacityAttributes.getDesired().ifPresent(newCapacityBuilder::withDesired);
        capacityAttributes.getMax().ifPresent(newCapacityBuilder::withMax);
        capacityAttributes.getMin().ifPresent(newCapacityBuilder::withMin);
        return job.toBuilder().withJobDescriptor(changeServiceJobCapacity(job.getJobDescriptor(), newCapacityBuilder.build())).build();
    }

    public static <E extends JobDescriptorExt> JobDescriptor<E> changeSecurityGroups(JobDescriptor<E> jobDescriptor, List<String> securityGroups) {
        return jobDescriptor.but(jd -> jd.getContainer()
                .but(c -> c.getSecurityProfile().toBuilder().withSecurityGroups(securityGroups).build()));
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

    public static Task changeTaskStatus(Task task, TaskState taskState, String reasonCode, String reasonMessage, Clock clock) {
        TaskStatus newStatus = JobModel.newTaskStatus()
                .withState(taskState)
                .withReasonCode(reasonCode)
                .withReasonMessage(reasonMessage)
                .withTimestamp(clock.wallTime())
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

    public static <E extends JobDescriptorExt> Function<Job<E>, Job<E>> withApplicationName(String appName) {
        return job -> job.toBuilder().withJobDescriptor(job.getJobDescriptor().toBuilder().withApplicationName(appName).build()).build();
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

    public static <E extends JobDescriptorExt> Job<E> appendCallMetadataJobAttributes(Job<E> input, CallMetadata callMetadata) {
        // Add call metadata as job attribute
        Map<String, String> callMetadataAttribute = new HashMap<>();
        String callerId = callMetadata.getCallers().isEmpty()
                ? "unknown"
                : callMetadata.getCallers().get(0).getId();
        callMetadataAttribute.put(JobAttributes.JOB_ATTRIBUTES_CREATED_BY, callerId);
        callMetadataAttribute.put(JobAttributes.JOB_ATTRIBUTES_CALL_REASON, callMetadata.getCallReason());
        JobDescriptor<E> jobDescriptor = input.getJobDescriptor();
        Map<String, String> updatedAttributes = CollectionsExt.merge(jobDescriptor.getAttributes(), callMetadataAttribute);
        return input.toBuilder().withJobDescriptor(jobDescriptor.toBuilder().withAttributes(updatedAttributes).build()).build();
    }

    public static <E extends JobDescriptorExt> Job<E> updateJobAttributes(Job<E> input, Map<String, String> attributes) {
        JobDescriptor<E> jobDescriptor = input.getJobDescriptor();
        Map<String, String> updatedAttributes = CollectionsExt.merge(jobDescriptor.getAttributes(), attributes);
        return input.toBuilder().withJobDescriptor(jobDescriptor.toBuilder().withAttributes(updatedAttributes).build()).build();
    }

    public static <E extends JobDescriptorExt> Job<E> deleteJobAttributes(Job<E> input, Set<String> keys) {
        return input.toBuilder().withJobDescriptor(deleteJobAttributes(input.getJobDescriptor(), keys)).build();
    }

    public static <E extends JobDescriptorExt> JobDescriptor<E> deleteJobAttributes(JobDescriptor<E> input, Set<String> keys) {
        Map<String, String> updatedAttributes = CollectionsExt.copyAndRemove(input.getAttributes(), keys);
        return input.toBuilder().withAttributes(updatedAttributes).build();
    }

    public static <E extends JobDescriptorExt> JobDescriptor<E> appendJobSecurityAttributes(JobDescriptor<E> input, Map<String, String> attributes) {
        SecurityProfile securityProfile = input.getContainer().getSecurityProfile();
        Map<String, String> updatedAttributes = CollectionsExt.merge(securityProfile.getAttributes(), attributes);
        return input.toBuilder()
                .withContainer(input.getContainer().toBuilder()
                        .withSecurityProfile(securityProfile.toBuilder()
                                .withAttributes(updatedAttributes)
                                .build())
                        .build())
                .build();
    }

    public static <E extends JobDescriptorExt> JobDescriptor<E> deleteJobSecurityAttributes(JobDescriptor<E> input, Set<String> keys) {
        SecurityProfile securityProfile = input.getContainer().getSecurityProfile();
        Map<String, String> updatedAttributes = CollectionsExt.copyAndRemove(securityProfile.getAttributes(), keys);
        return input.toBuilder()
                .withContainer(input.getContainer().toBuilder()
                        .withSecurityProfile(securityProfile.toBuilder()
                                .withAttributes(updatedAttributes)
                                .build())
                        .build())
                .build();
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
        if (job.getStatus() == null) {
            return Optional.empty();
        }
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

    /**
     * Jobs can include a fractional runtime duration prediction in seconds, which are parsed with millisecond resolution.
     *
     * @return a duration (if present) with millisecond resolution
     * @see JobAttributes#JOB_ATTRIBUTES_RUNTIME_PREDICTION_SEC
     */
    public static Optional<Duration> getJobRuntimePrediction(Job job) {
        return getJobRuntimePrediction(job.getJobDescriptor());
    }

    /**
     * Jobs can include a fractional runtime duration prediction in seconds, which are parsed with millisecond resolution.
     *
     * @return a duration (if present) with millisecond resolution
     * @see JobAttributes#JOB_ATTRIBUTES_RUNTIME_PREDICTION_SEC
     */
    @SuppressWarnings("unchecked")
    public static Optional<Duration> getJobRuntimePrediction(JobDescriptor jobDescriptor) {
        if (!isBatchJob(jobDescriptor)) {
            return Optional.empty();
        }
        Map<String, String> attributes = ((JobDescriptor<BatchJobExt>) jobDescriptor).getAttributes();
        return Optional.ofNullable(attributes.get(JobAttributes.JOB_ATTRIBUTES_RUNTIME_PREDICTION_SEC))
                .flatMap(StringExt::parseDouble)
                .map(seconds -> ((long) (seconds * 1000))) // seconds -> milliseconds
                .map(Duration::ofMillis);
    }

    public static String getEffectiveCapacityGroup(Job job) {
        String capacityGroup = job.getJobDescriptor().getCapacityGroup();
        if (StringExt.isEmpty(capacityGroup)) {
            capacityGroup = job.getJobDescriptor().getApplicationName();
        }
        return StringExt.isEmpty(capacityGroup) ? DEFAULT_APPLICATION : capacityGroup;
    }

    public static <E extends JobDescriptorExt> JobDescriptor<E> jobWithEbsVolumes(JobDescriptor<E> jd, List<EbsVolume> ebsVolumes, Map<String, String> ebsVolumeAttributes) {
        return jd
                .but(d -> d.getContainer()
                        .but(c -> c.getContainerResources().toBuilder().withEbsVolumes(ebsVolumes).build()))
                .but(j -> ebsVolumeAttributes);
    }

    public static <E extends JobDescriptorExt> JobDescriptor<E> jobWithIpAllocations(JobDescriptor<E> jd, List<SignedIpAddressAllocation> ipAllocations) {
        return jd
                .but(j -> j.getContainer()
                        .but(c -> c.getContainerResources().toBuilder().withSignedIpAddressAllocations(ipAllocations).build()));
    }

    /**
     * Split tasks into groups with the same original id and order them by their resubmit order. Tasks that do not fit
     * into any such group are returned as a second parameter in the result. Keys in the returned map contain
     * task original ids. Lists start with the original task followed by its resubmits.
     */
    public static Pair<Map<String, List<Task>>, List<Task>> groupTasksByResubmitOrder(Collection<Task> tasks) {
        if (tasks.isEmpty()) {
            return Pair.of(Collections.emptyMap(), Collections.emptyList());
        }

        Map<String, Task> tasksById = new HashMap<>();
        Map<String, List<Task>> mapped = new HashMap<>();
        tasks.forEach(task -> {
            tasksById.put(task.getId(), task);
            if (task.getId().equals(task.getOriginalId())) {
                mapped.put(task.getId(), new ArrayList<>());
            }
        });

        Map<String, Task> parentIdToNextTask = new HashMap<>();
        tasks.forEach(task -> {
            String parentId = task.getResubmitOf().orElse(null);
            if (parentId != null && tasksById.containsKey(parentId)) {
                parentIdToNextTask.put(parentId, task);
            }
        });

        Map<String, Task> left = new HashMap<>(tasksById);
        mapped.forEach((originalId, list) -> {
            Task current = tasksById.get(originalId);
            while (current != null) {
                list.add(current);
                left.remove(current.getId());
                current = parentIdToNextTask.get(current.getId());
            }
        });
        List<Task> rejected = new ArrayList<>(left.values());
        return Pair.of(mapped, rejected);
    }
}
