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

package com.netflix.titus.ext.eureka.supervisor;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.titus.api.supervisor.service.LocalMasterReadinessResolver;

public class EurekaSupervisorModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(LocalMasterReadinessResolver.class).to(EurekaLocalMasterReadinessResolver.class);
    }

    @Provides
    @Singleton
    public EurekaSupervisorConfiguration getEurekaSupervisorConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(EurekaSupervisorConfiguration.class);
    }
}
