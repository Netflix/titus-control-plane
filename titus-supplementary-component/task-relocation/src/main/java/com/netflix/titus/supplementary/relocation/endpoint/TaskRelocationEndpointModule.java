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

package com.netflix.titus.supplementary.relocation.endpoint;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.governator.guice.jersey.GovernatorJerseySupportModule;
import com.netflix.titus.runtime.endpoint.common.grpc.GrpcEndpointConfiguration;
import com.netflix.titus.runtime.endpoint.resolver.HostCallerIdResolver;
import com.netflix.titus.runtime.endpoint.resolver.NoOpHostCallerIdResolver;
import com.netflix.titus.supplementary.relocation.endpoint.grpc.TaskRelocationGrpcServer;
import com.netflix.titus.supplementary.relocation.endpoint.rest.TaskRelocationJerseyModule;

public class TaskRelocationEndpointModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(HostCallerIdResolver.class).to(NoOpHostCallerIdResolver.class);

        // GRPC
        bind(TaskRelocationGrpcServer.class).asEagerSingleton();

        // REST
        install(new GovernatorJerseySupportModule());
        install(new TaskRelocationJerseyModule());
    }

    @Provides
    @Singleton
    public GrpcEndpointConfiguration getTaskRelocationEndpointConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(GrpcEndpointConfiguration.class, "titus.relocation.endpoint");
    }
}
