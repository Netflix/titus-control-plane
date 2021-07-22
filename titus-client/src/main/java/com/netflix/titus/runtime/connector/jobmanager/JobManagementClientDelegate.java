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

package com.netflix.titus.runtime.connector.jobmanager;

import java.util.Map;
import java.util.Set;

import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobProcesses;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudget;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.api.model.Page;
import com.netflix.titus.api.model.PageResult;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class JobManagementClientDelegate implements JobManagementClient {

    private final JobManagementClient delegate;

    public JobManagementClientDelegate(JobManagementClient delegate) {
        this.delegate = delegate;
    }

    @Override
    public Mono<String> createJob(JobDescriptor jobDescriptor, CallMetadata callMetadata) {
        return delegate.createJob(jobDescriptor, callMetadata);
    }

    @Override
    public Mono<Void> updateJobCapacity(String jobId, Capacity capacity, CallMetadata callMetadata) {
        return delegate.updateJobCapacity(jobId, capacity, callMetadata);
    }

    @Override
    public Mono<Void> updateJobStatus(String jobId, boolean enabled, CallMetadata callMetadata) {
        return delegate.updateJobStatus(jobId, enabled, callMetadata);
    }

    @Override
    public Mono<Void> updateJobProcesses(String jobId, ServiceJobProcesses serviceJobProcesses, CallMetadata callMetadata) {
        return delegate.updateJobProcesses(jobId, serviceJobProcesses, callMetadata);
    }

    @Override
    public Mono<Void> updateJobDisruptionBudget(String jobId, DisruptionBudget disruptionBudget, CallMetadata callMetadata) {
        return delegate.updateJobDisruptionBudget(jobId, disruptionBudget, callMetadata);
    }

    @Override
    public Mono<Job> findJob(String jobId) {
        return delegate.findJob(jobId);
    }

    @Override
    public Mono<PageResult<Job<?>>> findJobs(Map<String, String> filteringCriteria, Page page) {
        return delegate.findJobs(filteringCriteria, page);
    }

    @Override
    public Flux<JobManagerEvent<?>> observeJob(String jobId) {
        return delegate.observeJob(jobId);
    }

    @Override
    public Flux<JobManagerEvent<?>> observeJobs(Map<String, String> filteringCriteria) {
        return delegate.observeJobs(filteringCriteria);
    }

    @Override
    public Mono<Void> killJob(String jobId, CallMetadata callMetadata) {
        return delegate.killJob(jobId, callMetadata);
    }

    @Override
    public Mono<Void> updateJobAttributes(String jobId, Map<String, String> attributes, CallMetadata callMetadata) {
        return delegate.updateJobAttributes(jobId, attributes, callMetadata);
    }

    @Override
    public Mono<Void> deleteJobAttributes(String jobId, Set<String> attributeKeys, CallMetadata callMetadata) {
        return delegate.deleteJobAttributes(jobId, attributeKeys, callMetadata);
    }

    @Override
    public Mono<Task> findTask(String taskId) {
        return delegate.findTask(taskId);
    }

    @Override
    public Mono<PageResult<Task>> findTasks(Map<String, String> filteringCriteria, Page page) {
        return delegate.findTasks(filteringCriteria, page);
    }

    @Override
    public Mono<Void> killTask(String taskId, boolean shrink, CallMetadata callMetadata) {
        return delegate.killTask(taskId, shrink, callMetadata);
    }

    @Override
    public Mono<Void> updateTaskAttributes(String taskId, Map<String, String> attributes, CallMetadata callMetadata) {
        return delegate.updateTaskAttributes(taskId, attributes, callMetadata);
    }

    @Override
    public Mono<Void> deleteTaskAttributes(String taskId, Set<String> attributeKeys, CallMetadata callMetadata) {
        return delegate.deleteTaskAttributes(taskId, attributeKeys, callMetadata);
    }

    @Override
    public Mono<Void> moveTask(String sourceJobId, String targetJobId, String taskId, CallMetadata callMetadata) {
        return delegate.moveTask(sourceJobId, targetJobId, taskId, callMetadata);
    }
}
