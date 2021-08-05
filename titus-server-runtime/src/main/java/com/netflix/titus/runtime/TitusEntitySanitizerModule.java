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

package com.netflix.titus.runtime;

import javax.inject.Named;
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.archaius.api.Config;
import com.netflix.titus.api.agent.model.sanitizer.AgentSanitizerBuilder;
import com.netflix.titus.api.appscale.model.sanitizer.ScalingPolicySanitizerBuilder;
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

import static com.netflix.titus.api.agent.model.sanitizer.AgentSanitizerBuilder.AGENT_SANITIZER;
import static com.netflix.titus.api.appscale.model.sanitizer.ScalingPolicySanitizerBuilder.SCALING_POLICY_SANITIZER;
import static com.netflix.titus.api.jobmanager.model.job.sanitizer.JobSanitizerBuilder.JOB_PERMISSIVE_SANITIZER;
import static com.netflix.titus.api.jobmanager.model.job.sanitizer.JobSanitizerBuilder.JOB_STRICT_SANITIZER;
import static com.netflix.titus.api.loadbalancer.model.sanitizer.LoadBalancerSanitizerBuilder.LOAD_BALANCER_SANITIZER;
import static com.netflix.titus.api.scheduler.model.sanitizer.SchedulerSanitizerBuilder.SCHEDULER_SANITIZER;
/**
 *
 */
public class TitusEntitySanitizerModule extends AbstractModule {

    public static final String CUSTOM_JOB_CONFIGURATION_ROOT = "titusMaster.job.customConfiguration";
    public static final String DEFAULT_CUSTOM_JOB_CONFIGURATION_ROOT = "titusMaster.job.defaultCustomConfiguration";

    @Override
    protected void configure() {
    }

    @Provides
    @Singleton
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

    @Provides
    @Singleton
    @Named(SCALING_POLICY_SANITIZER)
    public EntitySanitizer getScalingPolicySanitizer() {
        return new ScalingPolicySanitizerBuilder().build();
    }

    @Provides
    @Singleton
    @Named(JOB_STRICT_SANITIZER)
    public EntitySanitizer getJobEntityStrictSanitizer(JobConfiguration jobConfiguration, JobAssertions jobAssertions) {
        return getJobEntitySanitizer(jobConfiguration, jobAssertions, VerifierMode.Strict);
    }

    @Provides
    @Singleton
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
    @Named(SCHEDULER_SANITIZER)
    public EntitySanitizer getSchedulerEntitySanitizer() {
        return new SchedulerSanitizerBuilder().build();
    }

    @Provides
    @Singleton
    public JobConfiguration getJobConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(JobConfiguration.class);
    }

    @Provides
    @Singleton
    public ObjectConfigurationResolver<JobDescriptor, CustomJobConfiguration> getJobObjectConfigurationResolver(Config configuration,
                                                                                                                ConfigProxyFactory factory) {
        return Archaius2Ext.newObjectConfigurationResolver(
                configuration.getPrefixedView(CUSTOM_JOB_CONFIGURATION_ROOT),
                JobDescriptor::getApplicationName,
                CustomJobConfiguration.class,
                factory.newProxy(CustomJobConfiguration.class, DEFAULT_CUSTOM_JOB_CONFIGURATION_ROOT)
        );
    }
}
