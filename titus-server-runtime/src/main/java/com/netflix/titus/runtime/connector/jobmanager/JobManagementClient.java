/*
 * Copyright 2019 Netflix, Inc.
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

import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobProcesses;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudget;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.api.model.Page;
import com.netflix.titus.api.model.PageResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface JobManagementClient {

    // ------------------------------------------------------------
    // Job operations

    Mono<String> createJob(JobDescriptor jobDescriptor, CallMetadata callMetadata);

    Mono<Void> updateJobCapacity(String jobId, Capacity capacity, CallMetadata callMetadata);

    Mono<Void> updateJobStatus(String jobId, boolean enabled, CallMetadata callMetadata);

    Mono<Void> updateJobProcesses(String jobId, ServiceJobProcesses serviceJobProcesses, CallMetadata callMetadata);

    Mono<Void> updateJobDisruptionBudget(String jobId, DisruptionBudget disruptionBudget, CallMetadata callMetadata);

    Mono<Job> findJob(String jobId);

    Mono<PageResult<Job<?>>> findJobs(Map<String, String> filteringCriteria, Page page);

    Flux<JobManagerEvent<?>> observeJob(String jobId);

    Flux<JobManagerEvent<?>> observeJobs(Map<String, String> filteringCriteria);

    Mono<Void> killJob(String jobId, CallMetadata callMetadata);

    Mono<Void> updateJobAttributes(String jobId, Map<String, String> attributes, CallMetadata callMetadata);

    Mono<Void> deleteJobAttributes(String jobId, Set<String> attributeKeys, CallMetadata callMetadata);

    // ------------------------------------------------------------
    // Task operations

    Mono<Task> findTask(String taskId);

    Mono<PageResult<Task>> findTasks(Map<String, String> filteringCriteria, Page page);

    Mono<Void> killTask(String taskId, boolean shrink, CallMetadata callMetadata);

    Mono<Void> updateTaskAttributes(String taskId, Map<String, String> attributes, CallMetadata callMetadata);

    Mono<Void> deleteTaskAttributes(String taskId, Set<String> attributeKeys, CallMetadata callMetadata);

    Mono<Void> moveTask(String sourceJobId, String targetJobId, String taskId, CallMetadata callMetadata);
}
