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

package io.netflix.titus.gateway.service.v3;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import io.netflix.titus.gateway.service.v3.internal.DefaultAgentManagementService;
import io.netflix.titus.gateway.service.v3.internal.DefaultAutoScalingService;
import io.netflix.titus.gateway.service.v3.internal.DefaultJobManagementService;
import io.netflix.titus.gateway.service.v3.internal.DefaultTitusManagementService;

public class V3ServiceModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(JobManagementService.class).to(DefaultJobManagementService.class);
        bind(AutoScalingService.class).to(DefaultAutoScalingService.class);
        bind(TitusManagementService.class).to(DefaultTitusManagementService.class);
        bind(AgentManagementService.class).to(DefaultAgentManagementService.class);
    }

    @Provides
    @Singleton
    public GrpcClientConfiguration getGrpcClientConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(GrpcClientConfiguration.class);
    }
}
