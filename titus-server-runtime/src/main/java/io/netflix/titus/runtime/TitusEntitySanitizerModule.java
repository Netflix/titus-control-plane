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

package io.netflix.titus.runtime;

import javax.inject.Named;
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import io.netflix.titus.api.agent.model.sanitizer.AgentSanitizerBuilder;
import io.netflix.titus.api.jobmanager.model.job.sanitizer.JobConfiguration;
import io.netflix.titus.api.jobmanager.model.job.sanitizer.JobSanitizerBuilder;
import io.netflix.titus.api.loadbalancer.model.sanitizer.LoadBalancerSanitizerBuilder;
import io.netflix.titus.api.model.ResourceDimension;
import io.netflix.titus.common.model.sanitizer.EntitySanitizer;

import static io.netflix.titus.api.loadbalancer.store.LoadBalancerStore.LOAD_BALANCER_SANITIZER;

/**
 */
public class TitusEntitySanitizerModule extends AbstractModule {

    public static final String JOB_SANITIZER = "job";
    public static final String AGENT_SANITIZER = "agent";

    // TODO Compute this dynamically from the available instance types within a tier
    private static final ResourceDimension MAX_CONTAINER_SIZE = new ResourceDimension(64, 16, 256_000_000, 256_000_000, 10_000);

    @Override
    protected void configure() {
    }

    @Provides
    @Singleton
    @Named(JOB_SANITIZER)
    public EntitySanitizer getJobEntitySanitizer(JobConfiguration jobConfiguration) {
        return new JobSanitizerBuilder()
                .withJobConstrainstConfiguration(jobConfiguration)
                .withMaxContainerSizeResolver(capacityGroup -> MAX_CONTAINER_SIZE)
                .build();
    }

    @Provides
    @Singleton
    @Named(AGENT_SANITIZER)
    public EntitySanitizer getAgentEntitySanitizer() {
        return new AgentSanitizerBuilder().build();
    }

    @Provides
    @Singleton
    @Named(LOAD_BALANCER_SANITIZER)
    public EntitySanitizer getLoadBalancerEntitySanitizer() {
        return new LoadBalancerSanitizerBuilder().build();
    }

    @Provides
    @Singleton
    public JobConfiguration getJobConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(JobConfiguration.class);
    }
}
