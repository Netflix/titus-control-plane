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

package io.netflix.titus.testkit.model.job;

import java.util.UUID;

import io.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.JobModel;
import io.netflix.titus.api.jobmanager.model.job.JobState;
import io.netflix.titus.api.jobmanager.model.job.JobStatus;
import io.netflix.titus.api.jobmanager.model.job.ServiceJobTask;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.common.data.generator.DataGenerator;

import static io.netflix.titus.common.data.generator.DataGenerator.range;
import static io.netflix.titus.common.data.generator.DataGenerator.zip;

/**
 */
public class JobGenerator {

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
        return numbers.map(n -> String.format("%s-Task#%09d", jobId.substring(14), n));
    }

    /**
     * Generates a sequence of batch jobs for the given job descriptor.
     */
    public static DataGenerator<Job<BatchJobExt>> batchJobs(JobDescriptor<BatchJobExt> jobDescriptor) {
        return jobIds().map(jobId -> JobModel.<BatchJobExt>newJob()
                .withId(jobId)
                .withStatus(JobStatus.newBuilder().withState(JobState.Accepted).build())
                .withJobDescriptor(jobDescriptor)
                .build());
    }

    /**
     * Generates a sequence of service jobs for the given job descriptor.
     */
    public static DataGenerator<Job<ServiceJobExt>> serviceJobs(JobDescriptor<ServiceJobExt> jobDescriptor) {
        return jobIds().map(jobId -> JobModel.<ServiceJobExt>newJob()
                .withId(jobId)
                .withStatus(JobStatus.newBuilder().withState(JobState.Accepted).build())
                .withJobDescriptor(jobDescriptor)
                .build());
    }

    /**
     * Generates a sequence of tasks for a given job.
     */
    public static DataGenerator<BatchJobTask> batchTasks(Job<BatchJobExt> batchJob) {
        int size = batchJob.getJobDescriptor().getExtensions().getSize();
        return zip(taskIds(batchJob.getId(), range(1)), range(0, size)).map(p -> {
            String taskId = p.getLeft();
            int taskIndex = p.getRight().intValue();
            return BatchJobTask.newBuilder()
                    .withId(taskId)
                    .withOriginalId(taskId)
                    .withStatus(TaskStatus.newBuilder().withState(TaskState.Accepted).build())
                    .withJobId(batchJob.getId())
                    .withIndex(taskIndex)
                    .build();
        });
    }

    /**
     * Generates a sequence of tasks for a given job.
     */
    public static DataGenerator<ServiceJobTask> serviceTasks(Job<ServiceJobExt> serviceJob) {
        int size = serviceJob.getJobDescriptor().getExtensions().getCapacity().getDesired();
        return zip(taskIds(serviceJob.getId(), range(1)), range(0, size)).map(p -> {
            String taskId = p.getLeft();
            return ServiceJobTask.newBuilder()
                    .withId(taskId)
                    .withOriginalId(taskId)
                    .withStatus(TaskStatus.newBuilder().withState(TaskState.Accepted).build())
                    .withJobId(serviceJob.getId())
                    .build();
        });
    }
}
