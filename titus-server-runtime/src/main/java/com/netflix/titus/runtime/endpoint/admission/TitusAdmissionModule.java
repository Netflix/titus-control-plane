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

package com.netflix.titus.runtime.endpoint.admission;

import java.util.Arrays;
import java.util.Collections;
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.runtime.TitusEntitySanitizerModule;

/**
 * This module provides dependencies for Titus validation ({@link AdmissionValidator} and {@link AdmissionSanitizer})
 * which is beyond syntactic validation. See {@link TitusEntitySanitizerModule} for syntactic sanitization and validation.
 */
public class TitusAdmissionModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(new TypeLiteral<AdmissionValidator<JobDescriptor>>() {
        }).to(AggregatingValidator.class);
    }

    @Provides
    @Singleton
    TitusValidatorConfiguration getConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(TitusValidatorConfiguration.class);
    }

    @Provides
    @Singleton
    public AggregatingValidator getJobValidator(TitusValidatorConfiguration configuration,
                                                JobIamValidator jobIamValidator,
                                                Registry registry) {
        return new AggregatingValidator(configuration, registry, Collections.singletonList(jobIamValidator));
    }

    @Provides
    @Singleton
    public AggregatingSanitizer getJobSanitizer(TitusValidatorConfiguration configuration,
                                                JobImageSanitizer jobImageSanitizer,
                                                JobIamValidator jobIamSanitizer) {
        return new AggregatingSanitizer(configuration, Arrays.asList(jobImageSanitizer, jobIamSanitizer));
    }
}
