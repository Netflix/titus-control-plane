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

package com.netflix.titus.runtime.connector.registry;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.titus.runtime.endpoint.validator.JobImageValidatorConfiguration;

public class TitusContainerRegistryModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(RegistryClient.class).to(DefaultDockerRegistryClient.class);
    }

    @Provides
    @Singleton
    public TitusRegistryClientConfiguration getTitusRegistryConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(TitusRegistryClientConfiguration.class);
    }

    @Provides
    @Singleton
    public JobImageValidatorConfiguration getJobImageValidatorConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(JobImageValidatorConfiguration.class);
    }
}
