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

package io.netflix.titus.api.loadbalancer.model.sanitizer;

import java.util.Optional;
import javax.inject.Inject;

import io.netflix.titus.api.connector.cloud.LoadBalancerClient;
import io.netflix.titus.api.jobmanager.model.job.ContainerResources;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.JobState;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.api.loadbalancer.store.LoadBalancerStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultLoadBalancerValidator implements LoadBalancerValidator {
    private static final Logger logger = LoggerFactory.getLogger(DefaultLoadBalancerValidator.class);

    private final V3JobOperations v3JobOperations;
    private final LoadBalancerClient loadBalancerClient;
    private final LoadBalancerStore loadBalancerStore;
    private final LoadBalancerValidationConfiguration loadBalancerValidationConfiguration;

    @Inject
    public DefaultLoadBalancerValidator(V3JobOperations v3JobOperations,
                                        LoadBalancerClient loadBalancerClient,
                                        LoadBalancerStore loadBalancerStore,
                                        LoadBalancerValidationConfiguration loadBalancerValidationConfiguration) {
        this.v3JobOperations = v3JobOperations;
        this.loadBalancerClient = loadBalancerClient;
        this.loadBalancerStore = loadBalancerStore;
        this.loadBalancerValidationConfiguration = loadBalancerValidationConfiguration;
    }

    @Override
    public void validateJobId(String jobId) throws Exception {
        // Job must exist
        Optional<Job<?>> jobOptional = v3JobOperations.getJob(jobId);
        if (!jobOptional.isPresent()) {
            throw new Exception(String.format("Job %s does not exist", jobId));
        }

        // Job must be active
        Job<?> job = jobOptional.get();
        JobState state = job.getStatus().getState();
        if (state != JobState.Accepted) {
            throw new Exception(String.format("Job %s is in state %s, should be %s",
                    jobId, state.name(), JobState.Accepted.name()));
        }

        // Must be a service job
        JobDescriptor.JobDescriptorExt extensions = job.getJobDescriptor().getExtensions();
        if (!(extensions instanceof ServiceJobExt)) {
            throw new Exception(String.format("Job %s is NOT of type service", jobId));
        }

        // Must have routable IP
        ContainerResources containerResources = job.getJobDescriptor().getContainer().getContainerResources();
        if (!containerResources.isAllocateIP()) {
            throw new Exception(String.format("Job must request a routable IP"));
        }

        // Job should have less than max current load balancer associations
        int numLoadBalancers = loadBalancerStore.retrieveLoadBalancersForJob(jobId).count().toBlocking().single();
        if (numLoadBalancers > loadBalancerValidationConfiguration.getMaxLoadBalancersPerJob()) {
            throw new Exception(String.format("Number of load balancers for Job %s exceeds max of %s", job, loadBalancerValidationConfiguration.getMaxLoadBalancersPerJob()));
        }
    }

    @Override
    public void validateLoadBalancer(String loadBalancerId) throws Exception {
        // Load balancer ID must exist
        // Load balancer ID must accept IP targets
        logger.info("Validating load balancer {}!", loadBalancerId);

    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private V3JobOperations v3JobOperations;
        private LoadBalancerClient loadBalancerClient;
        private LoadBalancerStore loadBalancerStore;
        private LoadBalancerValidationConfiguration loadBalancerValidationConfiguration;

        private Builder() {
        }

        public static Builder aLoadBalancerValidator() {
            return new Builder();
        }

        public Builder withV3JobOperations(V3JobOperations v3JobOperations) {
            this.v3JobOperations = v3JobOperations;
            return this;
        }

        public Builder withLoadBalancerClient(LoadBalancerClient loadBalancerClient) {
            this.loadBalancerClient = loadBalancerClient;
            return this;
        }

        public Builder withLoadBalancerStore(LoadBalancerStore loadBalancerStore) {
            this.loadBalancerStore = loadBalancerStore;
            return this;
        }

        public Builder withLoadBalancerConfiguration(LoadBalancerValidationConfiguration loadBalancerValidationConfiguration) {
            this.loadBalancerValidationConfiguration = loadBalancerValidationConfiguration;
            return this;
        }

        public DefaultLoadBalancerValidator build() {
            return new DefaultLoadBalancerValidator(v3JobOperations, loadBalancerClient, loadBalancerStore, loadBalancerValidationConfiguration);
        }
    }
}
