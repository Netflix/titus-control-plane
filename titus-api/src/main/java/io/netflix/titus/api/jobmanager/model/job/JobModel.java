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

import io.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.api.jobmanager.model.job.migration.SelfManagedMigrationPolicy;
import io.netflix.titus.api.jobmanager.model.job.migration.SystemDefaultMigrationPolicy;
import io.netflix.titus.api.jobmanager.model.job.retry.DelayedRetryPolicy;
import io.netflix.titus.api.jobmanager.model.job.retry.DelayedRetryPolicyBuilder;
import io.netflix.titus.api.jobmanager.model.job.retry.ExponentialBackoffRetryPolicy;
import io.netflix.titus.api.jobmanager.model.job.retry.ExponentialBackoffRetryPolicyBuilder;
import io.netflix.titus.api.jobmanager.model.job.retry.ImmediateRetryPolicy;
import io.netflix.titus.api.jobmanager.model.job.retry.ImmediateRetryPolicyBuilder;

/**
 * A collection of factory, and help methods operating on job data model.
 */
public final class JobModel {

    private JobModel() {
    }

    public static Capacity.Builder newCapacity() {
        return Capacity.newBuilder();
    }

    public static ServiceJobProcesses.Builder newServiceJobProcesses() {
        return ServiceJobProcesses.newBuilder();
    }

    public static Capacity.Builder newCapacity(Capacity capacity) {
        return Capacity.newBuilder(capacity);
    }

    public static Image.Builder newImage() {
        return Image.newBuilder();
    }

    public static Image.Builder newImage(Image image) {
        return Image.newBuilder(image);
    }

    public static JobStatus.Builder newJobStatus() {
        return JobStatus.newBuilder();
    }

    public static JobStatus.Builder newJobStatus(JobStatus jobStatus) {
        return JobStatus.newBuilder(jobStatus);
    }

    public static TaskStatus.Builder newTaskStatus() {
        return TaskStatus.newBuilder();
    }

    public static TaskStatus.Builder newTaskStatus(TaskStatus taskStatus) {
        return TaskStatus.newBuilder(taskStatus);
    }

    public static ContainerResources.Builder newContainerResources() {
        return ContainerResources.newBuilder();
    }

    public static ContainerResources.Builder newContainerResources(ContainerResources containerResources) {
        return ContainerResources.newBuilder(containerResources);
    }

    public static SecurityProfile.Builder newSecurityProfile() {
        return SecurityProfile.newBuilder();
    }

    public static SecurityProfile.Builder newSecurityProfile(SecurityProfile securityProfile) {
        return SecurityProfile.newBuilder(securityProfile);
    }

    public static Container.Builder newContainer() {
        return Container.newBuilder();
    }

    public static Container.Builder newContainer(Container container) {
        return Container.newBuilder(container);
    }

    public static ImmediateRetryPolicyBuilder newImmediateRetryPolicy() {
        return ImmediateRetryPolicy.newBuilder();
    }

    public static DelayedRetryPolicyBuilder newDelayedRetryPolicy() {
        return DelayedRetryPolicy.newBuilder();
    }

    public static ExponentialBackoffRetryPolicyBuilder newExponentialBackoffRetryPolicy() {
        return ExponentialBackoffRetryPolicy.newBuilder();
    }

    public static SystemDefaultMigrationPolicy.Builder newSystemDefaultMigrationPolicy() {
        return SystemDefaultMigrationPolicy.newBuilder();
    }

    public static SelfManagedMigrationPolicy.Builder newSelfManagedMigrationPolicy() {
        return SelfManagedMigrationPolicy.newBuilder();
    }

    public static <E extends JobDescriptor.JobDescriptorExt> JobDescriptor.Builder<E> newJobDescriptor() {
        return JobDescriptor.newBuilder();
    }

    public static <E extends JobDescriptor.JobDescriptorExt> JobDescriptor.Builder<E> newJobDescriptor(JobDescriptor<E> jobDescriptor) {
        return JobDescriptor.newBuilder(jobDescriptor);
    }

    public static Owner.Builder newOwner() {
        return Owner.newBuilder();
    }

    public static Owner.Builder newOwner(Owner owner) {
        return Owner.newBuilder(owner);
    }

    public static JobGroupInfo.Builder newJobGroupInfo() {
        return JobGroupInfo.newBuilder();
    }

    public static JobGroupInfo.Builder newJobGroupInfo(JobGroupInfo jobGroupInfo) {
        return JobGroupInfo.newBuilder(jobGroupInfo);
    }

    public static <E extends JobDescriptor.JobDescriptorExt> Job.Builder<E> newJob() {
        return Job.newBuilder();
    }

    public static <E extends JobDescriptor.JobDescriptorExt> Job.Builder<E> newJob(Job<E> job) {
        return Job.newBuilder(job);
    }

    public static BatchJobExt.Builder newBatchJobExt() {
        return BatchJobExt.newBuilder();
    }

    public static BatchJobExt.Builder newBatchJobExt(BatchJobExt batchJobExt) {
        return BatchJobExt.newBuilder(batchJobExt);
    }

    public static BatchJobTask.Builder newBatchJobTask() {
        return BatchJobTask.newBuilder();
    }

    public static BatchJobTask.Builder newBatchJobTask(BatchJobTask task) {
        return BatchJobTask.newBuilder(task);
    }

    public static ServiceJobExt.Builder newServiceJobExt() {
        return ServiceJobExt.newBuilder();
    }

    public static ServiceJobExt.Builder newServiceJobExt(ServiceJobExt serviceJobExt) {
        return ServiceJobExt.newBuilder(serviceJobExt);
    }

    public static ServiceJobTask.Builder newServiceJobTask() {
        return ServiceJobTask.newBuilder();
    }

    public static ServiceJobTask.Builder newServiceJobTask(ServiceJobTask task) {
        return ServiceJobTask.newBuilder(task);
    }
}
