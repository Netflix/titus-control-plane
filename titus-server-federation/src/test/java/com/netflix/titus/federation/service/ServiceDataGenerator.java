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

package com.netflix.titus.federation.service;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.common.data.generator.DataGenerator;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.runtime.endpoint.common.EmptyLogStorageInfo;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import com.netflix.titus.testkit.model.job.JobGenerator;

class ServiceDataGenerator {

    private DataGenerator<Job<BatchJobExt>> batchJobs;
    private DataGenerator<Job<ServiceJobExt>> serviceJobs;

    ServiceDataGenerator(Clock clock, int tasksInJob) {
        batchJobs = JobGenerator.batchJobs(JobDescriptorGenerator.batchJobDescriptors().map(descriptor -> {
            BatchJobExt extensions = descriptor.getExtensions().toBuilder()
                    .withSize(tasksInJob)
                    .build();
            return descriptor.toBuilder().withExtensions(extensions).build();
        }).getValue(), clock);
        serviceJobs = JobGenerator.serviceJobs(JobDescriptorGenerator.serviceJobDescriptors().map(descriptor -> {
            ServiceJobExt extensions = descriptor.getExtensions().toBuilder()
                    .withCapacity(new Capacity(tasksInJob, tasksInJob, tasksInJob))
                    .build();
            return descriptor.toBuilder().withExtensions(extensions).build();
        }).getValue(), clock);
    }

    <T> T newBatchJob(Function<Job<BatchJobExt>, T> mapper) {
        return newBatchJobs(1, mapper).get(0);
    }

    Job<BatchJobExt> newBatchJob() {
        return newBatchJobs(1).get(0);
    }

    <T> List<T> newBatchJobs(int count, Function<Job<BatchJobExt>, T> mapper) {
        return newBatchJobs(count).stream().map(mapper).collect(Collectors.toList());
    }

    List<Job<BatchJobExt>> newBatchJobs(int count) {
        Pair<DataGenerator<Job<BatchJobExt>>, List<Job<BatchJobExt>>> generated = batchJobs.getAndApply(count);
        batchJobs = generated.getLeft();
        return generated.getRight();
    }

    <T> T newServiceJob(Function<Job<ServiceJobExt>, T> mapper) {
        return newServiceJobs(1, mapper).get(0);
    }

    <T> List<T> newServiceJobs(int count, Function<Job<ServiceJobExt>, T> mapper) {
        return newServiceJobs(count).stream().map(mapper).collect(Collectors.toList());
    }

    Job<ServiceJobExt> newServiceJob() {
        return newServiceJobs(1).get(0);
    }

    List<Job<ServiceJobExt>> newServiceJobs(int count) {
        Pair<DataGenerator<Job<ServiceJobExt>>, List<Job<ServiceJobExt>>> generated = serviceJobs.getAndApply(count);
        serviceJobs = generated.getLeft();
        return generated.getRight();
    }

    List<Task> newBatchJobWithTasks() {
        List<Task> generatedTasks = new ArrayList<>();
        batchJobs = batchJobs.apply(job -> {
            List<Task> tasks = JobGenerator.batchTasks(job)
                    .limit(JobFunctions.getJobDesiredSize(job))
                    .map(t -> GrpcJobManagementModelConverters.toGrpcTask(t, new EmptyLogStorageInfo<>()))
                    .toList();
            generatedTasks.addAll(tasks);
        }, 1);
        return generatedTasks;
    }

    List<Task> newServiceJobWithTasks() {
        List<Task> generatedTasks = new ArrayList<>();
        serviceJobs = serviceJobs.apply(job -> {
            List<Task> tasks = JobGenerator.serviceTasks(job)
                    .limit(JobFunctions.getJobDesiredSize(job))
                    .map(t -> GrpcJobManagementModelConverters.toGrpcTask(t, new EmptyLogStorageInfo<>()))
                    .toList();
            generatedTasks.addAll(tasks);
        }, 1);
        return generatedTasks;
    }

}
