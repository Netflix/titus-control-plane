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

package com.netflix.titus.api.loadbalancer.model.sanitizer;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.loadbalancer.service.LoadBalancerException;
import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.loadbalancer.service.LoadBalancerException;
import com.netflix.titus.api.loadbalancer.store.LoadBalancerStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class DefaultLoadBalancerJobValidator implements LoadBalancerJobValidator {
    private static final Logger logger = LoggerFactory.getLogger(DefaultLoadBalancerJobValidator.class);

    private final V3JobOperations v3JobOperations;
    private final LoadBalancerStore loadBalancerStore;
    private final LoadBalancerValidationConfiguration loadBalancerValidationConfiguration;

    @Inject
    public DefaultLoadBalancerJobValidator(V3JobOperations v3JobOperations,
                                           LoadBalancerStore loadBalancerStore,
                                           LoadBalancerValidationConfiguration loadBalancerValidationConfiguration) {
        this.v3JobOperations = v3JobOperations;
        this.loadBalancerStore = loadBalancerStore;
        this.loadBalancerValidationConfiguration = loadBalancerValidationConfiguration;
    }

    @Override
    public void validateJobId(String jobId) throws LoadBalancerException, JobManagerException {
        // Job must exist
        Job job = v3JobOperations.getJob(jobId).orElseThrow(() -> JobManagerException.v3JobNotFound(jobId));

        // Job must be active
        JobState state = job.getStatus().getState();
        if (state != JobState.Accepted) {
            throw JobManagerException.unexpectedJobState(job, JobState.Accepted);
        }

        // Must be a service job
        JobDescriptor.JobDescriptorExt extensions = job.getJobDescriptor().getExtensions();
        if (!(extensions instanceof ServiceJobExt)) {
            throw JobManagerException.notServiceJob(jobId);
        }

        // Must have routable IP
        ContainerResources containerResources = job.getJobDescriptor().getContainer().getContainerResources();
        if (!containerResources.isAllocateIP()) {
            throw LoadBalancerException.jobNotRoutableIp(jobId);
        }

        // Job should have less than max current load balancer associations
        int maxLoadBalancers = loadBalancerValidationConfiguration.getMaxLoadBalancersPerJob();
        int numLoadBalancers = loadBalancerStore.getNumLoadBalancersForJob(jobId);
        if (numLoadBalancers > maxLoadBalancers) {
            throw LoadBalancerException.jobMaxLoadBalancers(jobId, numLoadBalancers, maxLoadBalancers);
        }
    }
}
