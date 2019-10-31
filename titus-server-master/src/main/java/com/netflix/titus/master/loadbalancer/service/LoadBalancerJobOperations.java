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

package com.netflix.titus.master.loadbalancer.service;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import com.netflix.titus.api.loadbalancer.model.LoadBalancerTarget;
import rx.Observable;

/**
 * Wrapper for the V3 engines with some load balancer specific logic.
 */
class LoadBalancerJobOperations {
    private final V3JobOperations v3JobOperations;

    LoadBalancerJobOperations(V3JobOperations v3JobOperations) {
        this.v3JobOperations = v3JobOperations;
    }

    /**
     * Valid targets are tasks in the Started state that have ip addresses associated to them.
     *
     * @param jobLoadBalancer association
     * @throws JobManagerException when the job is not present anymore
     */
    List<LoadBalancerTarget> targetsForJob(JobLoadBalancer jobLoadBalancer) {
        return v3JobOperations.getTasks(jobLoadBalancer.getJobId()).stream()
                .filter(TaskHelpers::isStartedWithIp)
                .map(task -> new LoadBalancerTarget(
                        jobLoadBalancer.getLoadBalancerId(),
                        task.getId(),
                        task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP)
                ))
                .collect(Collectors.toList());
    }

    Observable<JobManagerEvent<?>> observeJobs() {
        return v3JobOperations.observeJobs();
    }

    Optional<Job<?>> getJob(String jobId) {
        return v3JobOperations.getJob(jobId);
    }
}
