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

package com.netflix.titus.testkit.model.job;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.JobModel;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.JobStatus;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobTask;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.common.data.generator.DataGenerator;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.common.util.time.Clocks;

import static com.netflix.titus.common.data.generator.DataGenerator.range;
import static com.netflix.titus.common.data.generator.DataGenerator.zip;

/**
 *
 */
public class JobGenerator {

    public static final JobDescriptor<JobDescriptor.JobDescriptorExt> EMPTY_JOB_DESCRIPTOR = JobModel.newJobDescriptor().build();

    /**
     * See {@link #jobIds(DataGenerator)}.
     */
    public static DataGenerator<String> jobIds() {
        return jobIds(range(1));
    }

    /**
     * Generates job UUID like strings, with readable job number (for example "a35b8764-f3fa-4d55-9c1f-Job#00000001")
     */
    public static DataGenerator<String> jobIds(DataGenerator<Long> numbers) {
        return numbers.map(n -> String.format("%sJob#%08d", UUID.randomUUID().toString().substring(0, 24), n));
    }

    /**
     * Generates task UUID like strings, with readable job/task numbers (for example "40df-8de3-Job#00000001-Task#000000001").
     */
    public static DataGenerator<String> taskIds(String jobId, DataGenerator<Long> numbers) {
        return numbers.map(n -> String.format("%s-Task#%09d", jobId, n));
    }

    /**
     * Generates sequence of jobs with empty job descriptor.
     */
    public static DataGenerator<Job> jobs() {
        return jobs(Clocks.system());
    }

    /**
     * Generates sequence of jobs with empty job descriptor.
     */
    public static DataGenerator<Job> jobs(Clock clock) {
        return jobIds().map(jobId ->
                JobModel.newJob()
                        .withId(jobId)
                        .withStatus(JobStatus.newBuilder()
                                .withTimestamp(clock.wallTime())
                                .withState(JobState.Accepted).build()
                        )
                        .withJobDescriptor(EMPTY_JOB_DESCRIPTOR)
                        .build());
    }

    /**
     * Generates a sequence of batch jobs for the given job descriptor.
     */
    public static DataGenerator<Job<BatchJobExt>> batchJobs(JobDescriptor<BatchJobExt> jobDescriptor) {
        return batchJobs(jobDescriptor, Clocks.system());
    }

    /**
     * Generates a sequence of batch jobs for the given job descriptor.
     */
    public static DataGenerator<Job<BatchJobExt>> batchJobs(JobDescriptor<BatchJobExt> jobDescriptor, Clock clock) {
        return jobIds().map(jobId -> JobModel.<BatchJobExt>newJob()
                .withId(jobId)
                .withStatus(JobStatus.newBuilder()
                        .withTimestamp(clock.wallTime())
                        .withState(JobState.Accepted).build())
                .withJobDescriptor(jobDescriptor)
                .build());
    }

    public static DataGenerator<Job<BatchJobExt>> batchJobsOfSize(int size) {
        return batchJobs(JobFunctions.changeBatchJobSize(JobDescriptorGenerator.oneTaskBatchJobDescriptor(), size));
    }


    public static DataGenerator<Job<BatchJobExt>> batchJobsOfSizeAndAttributes(int size, Map<String, String> jobAttributes) {
        JobDescriptor<BatchJobExt> jobDescriptor = JobFunctions.appendJobDescriptorAttributes(
                JobFunctions.changeBatchJobSize(
                        JobDescriptorGenerator.oneTaskBatchJobDescriptor(), size), jobAttributes);
        return batchJobs(jobDescriptor);
    }

    /**
     * Generates a sequence of service jobs for the given job descriptor.
     */
    public static DataGenerator<Job<ServiceJobExt>> serviceJobs(JobDescriptor<ServiceJobExt> jobDescriptor) {
        return serviceJobs(jobDescriptor, Clocks.system());
    }

    /**
     * Generates a sequence of service jobs for the given job descriptor.
     */
    public static DataGenerator<Job<ServiceJobExt>> serviceJobs(JobDescriptor<ServiceJobExt> jobDescriptor, Clock clock) {
        return jobIds().map(jobId -> JobModel.<ServiceJobExt>newJob()
                .withId(jobId)
                .withStatus(JobStatus.newBuilder()
                        .withTimestamp(clock.wallTime())
                        .withState(JobState.Accepted).build())
                .withJobDescriptor(jobDescriptor)
                .build());
    }

    /**
     * Generates a sequence of tasks for a given job.
     */
    public static DataGenerator<BatchJobTask> batchTasks(Job<BatchJobExt> batchJob) {
        int size = batchJob.getJobDescriptor().getExtensions().getSize();
        return zip(taskIds(batchJob.getId(), range(1)), range(0, size).loop()).map(p -> {
            String taskId = p.getLeft();
            int taskIndex = p.getRight().intValue();
            return BatchJobTask.newBuilder()
                    .withId(taskId)
                    .withOriginalId(taskId)
                    .withStatus(TaskStatus.newBuilder().withState(TaskState.Accepted).build())
                    .withJobId(batchJob.getId())
                    .withIndex(taskIndex)
                    .withTaskContext(Collections.singletonMap(TaskAttributes.TASK_ATTRIBUTES_TASK_INDEX, "" + taskIndex))
                    .build();
        });
    }

    /**
     * Generates a sequence of tasks for a given job.
     */
    public static DataGenerator<ServiceJobTask> serviceTasks(Job<ServiceJobExt> serviceJob) {
        int size = serviceJob.getJobDescriptor().getExtensions().getCapacity().getDesired();
        return taskIds(serviceJob.getId(), range(1, size + 1)).map(taskId ->
                ServiceJobTask.newBuilder()
                        .withId(taskId)
                        .withOriginalId(taskId)
                        .withStatus(TaskStatus.newBuilder().withState(TaskState.Accepted).build())
                        .withJobId(serviceJob.getId())
                        .build()
        );
    }

    public static Job<ServiceJobExt> oneServiceJob() {
        return serviceJobs(JobDescriptorGenerator.oneTaskServiceJobDescriptor()).getValue();
    }

    public static ServiceJobTask oneServiceTask() {
        return JobGenerator.serviceTasks(oneServiceJob()).getValue();
    }

    public static Job<BatchJobExt> oneBatchJob() {
        return batchJobs(JobDescriptorGenerator.oneTaskBatchJobDescriptor()).getValue();
    }

    public static BatchJobTask oneBatchTask() {
        return JobGenerator.batchTasks(oneBatchJob()).getValue();
    }
}
