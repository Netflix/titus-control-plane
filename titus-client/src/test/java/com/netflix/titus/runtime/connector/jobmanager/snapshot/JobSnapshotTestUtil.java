/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.runtime.connector.jobmanager.snapshot;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.common.data.generator.DataGenerator;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;

import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.ofServiceSize;

class JobSnapshotTestUtil {

    static JobSnapshot newSnapshot(JobSnapshotFactory factory, Pair<Job<?>, Map<String, Task>>... pairs) {
        Map<String, Job<?>> jobsById = new HashMap<>();
        Map<String, Map<String, Task>> tasksByJobId = new HashMap<>();
        for (Pair<Job<?>, Map<String, Task>> pair : pairs) {
            Job<?> job = pair.getLeft();
            jobsById.put(job.getId(), job);
            tasksByJobId.put(job.getId(), pair.getRight());
        }
        return factory.newSnapshot(jobsById, tasksByJobId);
    }

    static Pair<Job<?>, Map<String, Task>> newJobWithTasks(int jobIdx, int taskCount) {
        return (Pair) newBatchJobWithTasks(jobIdx, taskCount);
    }

    static Pair<Job<ServiceJobExt>, PMap<String, Task>> newServiceJobWithTasks(int jobIdx, int taskCount, int maxSize) {
        Job<ServiceJobExt> job = JobGenerator.serviceJobs(
                JobDescriptorGenerator.serviceJobDescriptors(ofServiceSize(maxSize)).getValue()
        ).getValue().toBuilder().withId("job#" + jobIdx).build();

        Map<String, Task> tasks = JobGenerator.serviceTasks(job).getValues(taskCount)
                .stream().collect(Collectors.toMap(Task::getId, t -> t));
        return Pair.of(job, HashTreePMap.from(tasks));
    }

    static Task newServiceTask(Job<ServiceJobExt> job, int taskIdx) {
        return JobGenerator.serviceTasks(job).getValue().toBuilder()
                .withId(job.getId() + "-Task#" + taskIdx)
                .build();
    }

    static Pair<Job<BatchJobExt>, PMap<String, Task>> newBatchJobWithTasks(int jobIdx, int taskCount) {
        Job<BatchJobExt> job = JobGenerator.batchJobs(JobDescriptorGenerator.batchJobDescriptor(taskCount)).getValue()
                .toBuilder().withId("job#" + jobIdx).build();
        Map<String, Task> tasks = JobGenerator.batchTasks(job).getValues(taskCount)
                .stream().collect(Collectors.toMap(Task::getId, t -> t));
        return Pair.of(job, HashTreePMap.from(tasks));
    }
}
