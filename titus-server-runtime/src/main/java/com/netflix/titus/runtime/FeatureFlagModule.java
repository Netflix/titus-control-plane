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

package com.netflix.titus.runtime;

import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.inject.Named;
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.titus.api.FeatureActivationConfiguration;
import com.netflix.titus.api.jobmanager.JobConstraints;
import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.RegExpExt;
import com.netflix.titus.common.util.feature.FeatureGuardWhiteListConfiguration;
import com.netflix.titus.common.util.feature.FeatureGuards;
import com.netflix.titus.common.util.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.api.FeatureRolloutPlans.ENVIRONMENT_VARIABLE_NAMES_STRICT_VALIDATION_FEATURE;
import static com.netflix.titus.api.FeatureRolloutPlans.JOB_ACTIVITY_PUBLISH_FEATURE;
import static com.netflix.titus.api.FeatureRolloutPlans.JOB_AUTHORIZATION_FEATURE;
import static com.netflix.titus.api.FeatureRolloutPlans.KUBE_SCHEDULER_FEATURE;
import static com.netflix.titus.api.FeatureRolloutPlans.SECURITY_GROUPS_REQUIRED_FEATURE;

public class FeatureFlagModule extends AbstractModule {

    private static final Logger logger = LoggerFactory.getLogger(FeatureFlagModule.class);

    @Override
    protected void configure() {
    }

    @Provides
    @Singleton
    public FeatureActivationConfiguration getFeatureActivationConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(FeatureActivationConfiguration.class);
    }

    /* *************************************************************************************************************
     * Security groups/IAM role required.
     */

    @Provides
    @Singleton
    @Named(SECURITY_GROUPS_REQUIRED_FEATURE)
    public Predicate<JobDescriptor> getSecurityGroupRequiredPredicate(@Named(SECURITY_GROUPS_REQUIRED_FEATURE) FeatureGuardWhiteListConfiguration configuration) {
        return FeatureGuards.toPredicate(
                FeatureGuards.fromField(
                        JobDescriptor::getApplicationName,
                        FeatureGuards.newWhiteListFromConfiguration(configuration).build()
                )
        );
    }

    @Provides
    @Singleton
    @Named(SECURITY_GROUPS_REQUIRED_FEATURE)
    public FeatureGuardWhiteListConfiguration getSecurityGroupRequiredFeatureGuardConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(FeatureGuardWhiteListConfiguration.class, "titus.features.jobManager." + SECURITY_GROUPS_REQUIRED_FEATURE);
    }

    /* *************************************************************************************************************
     * Environment variables.
     *
     * Environment variable names set in {@link JobDescriptor} must conform to the rules defined in this spec:
     * http://pubs.opengroup.org/onlinepubs/9699919799/.
     *
     * This change was introduced in Q1/2019. The strict validation should be enforced on all clients by the end of Q2/2019.
     */

    @Provides
    @Singleton
    @Named(ENVIRONMENT_VARIABLE_NAMES_STRICT_VALIDATION_FEATURE)
    public Predicate<JobDescriptor> getEnvironmentVariableStrictValidationPredicate(@Named(ENVIRONMENT_VARIABLE_NAMES_STRICT_VALIDATION_FEATURE) FeatureGuardWhiteListConfiguration configuration) {
        return FeatureGuards.toPredicate(
                FeatureGuards.fromField(
                        JobDescriptor::getApplicationName,
                        FeatureGuards.newWhiteListFromConfiguration(configuration).build()
                )
        );
    }

    @Provides
    @Singleton
    @Named(ENVIRONMENT_VARIABLE_NAMES_STRICT_VALIDATION_FEATURE)
    public FeatureGuardWhiteListConfiguration getEnvironmentVariableStrictValidationConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(FeatureGuardWhiteListConfiguration.class, "titus.features.jobManager." + ENVIRONMENT_VARIABLE_NAMES_STRICT_VALIDATION_FEATURE);
    }

    /* *************************************************************************************************************
     * Job authorization
     *
     * This change was introduced in Q1/2019.
     */

    @Provides
    @Singleton
    @Named(JOB_AUTHORIZATION_FEATURE)
    public Predicate<String> getJobAuthorizationPredicate(@Named(JOB_AUTHORIZATION_FEATURE) FeatureGuardWhiteListConfiguration configuration) {
        return FeatureGuards.toPredicate(FeatureGuards.newWhiteListFromConfiguration(configuration).build());
    }

    @Provides
    @Singleton
    @Named(JOB_AUTHORIZATION_FEATURE)
    public FeatureGuardWhiteListConfiguration getJobAuthorizationFeatureGuardConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(FeatureGuardWhiteListConfiguration.class, "titus.features.jobManager." + JOB_AUTHORIZATION_FEATURE);
    }

    /* *************************************************************************************************************
     * Job activity history
     *
     * This change was introduced in Q1/2019.
     */

    @Provides
    @Singleton
    @Named(JOB_ACTIVITY_PUBLISH_FEATURE)
    public Predicate<JobDescriptor> getJobActivityPublishFeaturePredicate(@Named(JOB_ACTIVITY_PUBLISH_FEATURE) FeatureGuardWhiteListConfiguration configuration) {
        return FeatureGuards.toPredicate(
                FeatureGuards.fromField(
                        JobDescriptor::getApplicationName,
                        FeatureGuards.newWhiteListFromConfiguration(configuration).build()
                )
        );
    }

    @Provides
    @Singleton
    @Named(JOB_ACTIVITY_PUBLISH_FEATURE)
    public FeatureGuardWhiteListConfiguration getJobActivityPublishFeatureGuardConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(FeatureGuardWhiteListConfiguration.class, "titus.features.jobManager." + JOB_ACTIVITY_PUBLISH_FEATURE);
    }

    /* *************************************************************************************************************
     * Kube scheduler integration.
     *
     * This change was introduced in Q1/2020.
     */

    /**
     * TODO Kube scheduler does not support jobs that require: static IPs
     */
    @Provides
    @Singleton
    @Named(KUBE_SCHEDULER_FEATURE)
    public Predicate<Pair<JobDescriptor, ApplicationSLA>> getKubeSchedulerFeaturePredicate(ConfigProxyFactory factory) {
        KubeSchedulerFeatureConfiguration configuration = factory.newProxy(KubeSchedulerFeatureConfiguration.class);

        FeatureGuardWhiteListConfiguration applicationConfiguration = factory.newProxy(FeatureGuardWhiteListConfiguration.class, "titus.features.jobManager." + KUBE_SCHEDULER_FEATURE + "ByApplication");
        FeatureGuardWhiteListConfiguration capacityGroupConfiguration = factory.newProxy(FeatureGuardWhiteListConfiguration.class, "titus.features.jobManager." + KUBE_SCHEDULER_FEATURE + "ByCapacityGroup");
        FeatureGuardWhiteListConfiguration tierConfiguration = factory.newProxy(FeatureGuardWhiteListConfiguration.class, "titus.features.jobManager." + KUBE_SCHEDULER_FEATURE + "ByTier");
        FeatureGuardWhiteListConfiguration jobTypeConfiguration = factory.newProxy(FeatureGuardWhiteListConfiguration.class, "titus.features.jobManager." + KUBE_SCHEDULER_FEATURE + "ByJobType");
        FeatureGuardWhiteListConfiguration jobAttributeConfiguration = factory.newProxy(FeatureGuardWhiteListConfiguration.class, "titus.features.jobManager." + KUBE_SCHEDULER_FEATURE + "ByJobAttribute");

        Predicate<Pair<JobDescriptor, ApplicationSLA>> routingPredicate = FeatureGuards.toPredicate(
                FeatureGuards.fromField(p -> p.getLeft().getApplicationName(), FeatureGuards.newWhiteListFromConfiguration(applicationConfiguration).build()),
                FeatureGuards.fromField(p -> p.getRight().getAppName(), FeatureGuards.newWhiteListFromConfiguration(capacityGroupConfiguration).build()),
                FeatureGuards.fromField(p -> p.getRight().getTier().name(), FeatureGuards.newWhiteListFromConfiguration(tierConfiguration).build()),
                FeatureGuards.fromField(p -> JobFunctions.isServiceJob(p.getLeft()) ? "service" : "batch", FeatureGuards.newWhiteListFromConfiguration(jobTypeConfiguration).build()),
                FeatureGuards.fromMap(p -> p.getLeft().getAttributes(), FeatureGuards.newWhiteListFromConfiguration(jobAttributeConfiguration).build())
        );

        Function<String, Matcher> enabledMachineTypes = RegExpExt.dynamicMatcher(configuration::getEnabledMachineTypes,
                "titus.features.jobManager." + KUBE_SCHEDULER_FEATURE + "EnabledMachineTypes", Pattern.DOTALL, logger);

        return p -> {
            JobDescriptor<?> jobDescriptor = p.getLeft();
            ContainerResources resources = jobDescriptor.getContainer().getContainerResources();

            // Jobs with static IP addresses are not allowed.
            if (!CollectionsExt.isNullOrEmpty(resources.getSignedIpAddressAllocations())) {
                return false;
            }

            // Job should not be scheduled by KubeScheduler
            if (FeatureFlagUtil.isNoKubeSchedulerMigration(jobDescriptor)) {
                return false;
            }

            // Check GPU jobs
            if (!configuration.isGpuEnabled() && resources.getGpu() > 0) {
                return false;
            }

            // Check if the machine type is enabled
            String machineType = JobFunctions.findHardConstraint(jobDescriptor, JobConstraints.MACHINE_TYPE).orElse(null);
            if (machineType != null && !enabledMachineTypes.apply(machineType).matches()) {
                return false;
            }

            return routingPredicate.test(p);
        };
    }
}
