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

package com.netflix.titus.api;

import java.util.function.Predicate;
import javax.inject.Named;
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.common.util.feature.FeatureGuardWhiteListConfiguration;
import com.netflix.titus.common.util.feature.FeatureGuards;

public class FeatureFlagModule extends AbstractModule {

    public static final String DISRUPTION_BUDGET_FEATURE = "disruptionBudget";

    @Override
    protected void configure() {
    }

    @Provides
    @Singleton
    @Named(DISRUPTION_BUDGET_FEATURE)
    public Predicate<JobDescriptor> getDisruptionBudgetEnabledPredicate(@Named(DISRUPTION_BUDGET_FEATURE) FeatureGuardWhiteListConfiguration configuration) {
        return FeatureGuards.toPredicate(
                FeatureGuards.fromField(
                        JobDescriptor::getApplicationName,
                        FeatureGuards.newWhiteListFromConfiguration(configuration).build()
                )
        );
    }

    @Provides
    @Singleton
    @Named(DISRUPTION_BUDGET_FEATURE)
    public FeatureGuardWhiteListConfiguration getDisruptionBudgetFeatureGuardConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(FeatureGuardWhiteListConfiguration.class, "titusMaster.jobManager.features.disruptionBudget");
    }
}
