/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.runtime;

import java.time.Duration;
import javax.inject.Named;

import com.netflix.titus.api.agent.model.sanitizer.AgentSanitizerBuilder;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.sanitizer.CustomJobConfiguration;
import com.netflix.titus.api.jobmanager.model.job.sanitizer.JobAssertions;
import com.netflix.titus.api.jobmanager.model.job.sanitizer.JobConfiguration;
import com.netflix.titus.api.jobmanager.model.job.sanitizer.JobSanitizerBuilder;
import com.netflix.titus.api.loadbalancer.model.sanitizer.LoadBalancerSanitizerBuilder;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.scheduler.model.sanitizer.SchedulerSanitizerBuilder;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.common.model.sanitizer.VerifierMode;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.common.util.archaius2.ObjectConfigurationResolver;
import com.netflix.titus.common.util.closeable.CloseableDependency;
import com.netflix.titus.common.util.closeable.CloseableReference;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import reactor.core.publisher.Flux;

import static com.netflix.titus.api.agent.model.sanitizer.AgentSanitizerBuilder.AGENT_SANITIZER;
import static com.netflix.titus.api.jobmanager.model.job.sanitizer.JobSanitizerBuilder.JOB_PERMISSIVE_SANITIZER;
import static com.netflix.titus.api.jobmanager.model.job.sanitizer.JobSanitizerBuilder.JOB_STRICT_SANITIZER;
import static com.netflix.titus.api.loadbalancer.model.sanitizer.LoadBalancerSanitizerBuilder.LOAD_BALANCER_SANITIZER;
import static com.netflix.titus.api.scheduler.model.sanitizer.SchedulerSanitizerBuilder.SCHEDULER_SANITIZER;

@Configuration
public class TitusEntitySanitizerComponent {

    public static final String CUSTOM_JOB_CONFIGURATION_ROOT = "titusMaster.job.customConfiguration";
    public static final String DEFAULT_CUSTOM_JOB_CONFIGURATION_ROOT = "titusMaster.job.defaultCustomConfiguration";

    public static final String JOB_CONFIGURATION_RESOLVER = "jobConfigurationResolver";

    @Bean
    public JobConfiguration getJobConfiguration(Environment environment) {
        return Archaius2Ext.newConfiguration(JobConfiguration.class, environment);
    }

    @Bean
    public JobAssertions getJobAssertions(JobConfiguration jobConfiguration) {
        return new JobAssertions(
                jobConfiguration,
                capacityGroup -> ResourceDimension.newBuilder()
                        .withCpus(jobConfiguration.getCpuMax())
                        .withGpu(jobConfiguration.getGpuMax())
                        .withMemoryMB(jobConfiguration.getMemoryMegabytesMax())
                        .withDiskMB(jobConfiguration.getDiskMegabytesMax())
                        .withNetworkMbs(jobConfiguration.getNetworkMbpsMax())
                        .build()
        );
    }

    @Bean
    @Named(JOB_STRICT_SANITIZER)
    public EntitySanitizer getJobEntityStrictSanitizer(JobConfiguration jobConfiguration, JobAssertions jobAssertions) {
        return getJobEntitySanitizer(jobConfiguration, jobAssertions, VerifierMode.Strict);
    }

    @Bean
    @Named(JOB_PERMISSIVE_SANITIZER)
    public EntitySanitizer getJobEntityPermissiveSanitizer(JobConfiguration jobConfiguration, JobAssertions jobAssertions) {
        return getJobEntitySanitizer(jobConfiguration, jobAssertions, VerifierMode.Permissive);
    }

    private EntitySanitizer getJobEntitySanitizer(JobConfiguration jobConfiguration, JobAssertions jobAssertions, VerifierMode verifierMode) {
        return new JobSanitizerBuilder()
                .withVerifierMode(verifierMode)
                .withJobConstraintConfiguration(jobConfiguration)
                .withJobAsserts(jobAssertions)
                .build();
    }

    @Bean
    @Named(AGENT_SANITIZER)
    public EntitySanitizer getAgentEntitySanitizer() {
        return new AgentSanitizerBuilder().build();
    }

    @Bean
    @Named(LOAD_BALANCER_SANITIZER)
    public EntitySanitizer getLoadBalancerEntitySanitizer() {
        return new LoadBalancerSanitizerBuilder().build();
    }

    @Bean
    @Named(SCHEDULER_SANITIZER)
    public EntitySanitizer getSchedulerEntitySanitizer() {
        return new SchedulerSanitizerBuilder().build();
    }

    @Bean
    @Named(JOB_CONFIGURATION_RESOLVER)
    public CloseableDependency<ObjectConfigurationResolver<JobDescriptor, CustomJobConfiguration>> getJobObjectConfigurationResolverCloseable(Environment environment) {
        CloseableReference<ObjectConfigurationResolver<JobDescriptor, CustomJobConfiguration>> ref = Archaius2Ext.newObjectConfigurationResolver(
                CUSTOM_JOB_CONFIGURATION_ROOT,
                environment,
                JobDescriptor::getApplicationName,
                CustomJobConfiguration.class,
                Archaius2Ext.newConfiguration(CustomJobConfiguration.class, DEFAULT_CUSTOM_JOB_CONFIGURATION_ROOT, environment),
                Flux.interval(Duration.ofSeconds(1))
        );
        return CloseableDependency.of(ref);
    }

    @Bean
    public ObjectConfigurationResolver<JobDescriptor, CustomJobConfiguration> getJobObjectConfigurationResolver(
            @Named(JOB_CONFIGURATION_RESOLVER) CloseableDependency<ObjectConfigurationResolver<JobDescriptor, CustomJobConfiguration>> closeableDependency) {
        return closeableDependency.get();
    }
}
