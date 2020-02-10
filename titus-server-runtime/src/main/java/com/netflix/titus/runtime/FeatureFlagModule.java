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

import java.util.function.Predicate;
import javax.inject.Named;
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.titus.api.FeatureActivationConfiguration;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.common.util.feature.FeatureGuardWhiteListConfiguration;
import com.netflix.titus.common.util.feature.FeatureGuards;

import static com.netflix.titus.api.FeatureRolloutPlans.ENVIRONMENT_VARIABLE_NAMES_STRICT_VALIDATION_FEATURE;
import static com.netflix.titus.api.FeatureRolloutPlans.JOB_ACTIVITY_PUBLISH_FEATURE;
import static com.netflix.titus.api.FeatureRolloutPlans.JOB_AUTHORIZATION_FEATURE;
import static com.netflix.titus.api.FeatureRolloutPlans.KUBE_SCHEDULER_FEATURE;
import static com.netflix.titus.api.FeatureRolloutPlans.SECURITY_GROUPS_REQUIRED_FEATURE;

public class FeatureFlagModule extends AbstractModule {

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

    @Provides
    @Singleton
    @Named(KUBE_SCHEDULER_FEATURE)
    public Predicate<JobDescriptor> getKubeSchedulerFeaturePredicate(@Named(KUBE_SCHEDULER_FEATURE) FeatureGuardWhiteListConfiguration configuration) {
        return FeatureGuards.toPredicate(
                FeatureGuards.fromField(
                        JobDescriptor::getApplicationName,
                        FeatureGuards.newWhiteListFromConfiguration(configuration).build()
                )
        );
    }

    @Provides
    @Singleton
    @Named(KUBE_SCHEDULER_FEATURE)
    public FeatureGuardWhiteListConfiguration getKubeSchedulerFeatureGuardConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(FeatureGuardWhiteListConfiguration.class, "titus.features.jobManager." + KUBE_SCHEDULER_FEATURE);
    }
}
