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

package com.netflix.titus.gateway.startup;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.governator.guice.jersey.GovernatorJerseySupportModule;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.internal.DefaultTitusRuntime;
import com.netflix.titus.common.util.archaius2.Archaius2ConfigurationLogger;
import com.netflix.titus.common.util.code.CodeInvariants;
import com.netflix.titus.common.util.code.CompositeCodeInvariants;
import com.netflix.titus.common.util.code.LoggingCodeInvariants;
import com.netflix.titus.common.util.code.SpectatorCodeInvariants;
import com.netflix.titus.common.util.guice.ContainerEventBusModule;
import com.netflix.titus.gateway.connector.titusmaster.TitusMasterConnectorModule;
import com.netflix.titus.gateway.endpoint.GrpcModule;
import com.netflix.titus.gateway.endpoint.JerseyModule;
import com.netflix.titus.gateway.service.v2.V2ServiceModule;
import com.netflix.titus.gateway.service.v3.V3ServiceModule;
import com.netflix.titus.gateway.store.StoreModule;
import com.netflix.titus.runtime.TitusEntitySanitizerModule;
import com.netflix.titus.runtime.endpoint.common.EmptyLogStorageInfo;
import com.netflix.titus.runtime.endpoint.common.LogStorageInfo;
import com.netflix.titus.runtime.endpoint.resolver.HostCallerIdResolver;
import com.netflix.titus.runtime.endpoint.resolver.NoOpHostCallerIdResolver;

// Common module dependencies
// Server dependencies

/**
 * This is the "main" module where we wire everything up. If you see this module getting overly
 * complex, it's a good idea to break things off into separate ones and install them here instead.
 */

public final class TitusGatewayModule extends AbstractModule {

    public static final TypeLiteral<LogStorageInfo<Task>> V3_LOG_STORAGE_INFO =
            new TypeLiteral<LogStorageInfo<Task>>() {
            };

    @Override
    protected void configure() {

        bind(Archaius2ConfigurationLogger.class).asEagerSingleton();
        bind(Registry.class).toInstance(new DefaultRegistry());

        install(new ContainerEventBusModule());

        install(new TitusEntitySanitizerModule());

        install(new GovernatorJerseySupportModule());
        install(new JerseyModule());

        bind(HostCallerIdResolver.class).to(NoOpHostCallerIdResolver.class);

        install(new GrpcModule());
        install(new TitusMasterConnectorModule());

        bind(V3_LOG_STORAGE_INFO).toInstance(EmptyLogStorageInfo.INSTANCE);
        install(new V2ServiceModule());
        install(new V3ServiceModule());

        install(new StoreModule());
    }

    @Provides
    @Singleton
    TitusGatewayConfiguration getConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(TitusGatewayConfiguration.class);
    }

    @Provides
    @Singleton
    public TitusRuntime getTitusRuntime(Registry registry) {
        CodeInvariants codeInvariants = new CompositeCodeInvariants(
                LoggingCodeInvariants.getDefault(),
                new SpectatorCodeInvariants(registry.createId("titus.runtime.invariant.violations"), registry)
        );
        return new DefaultTitusRuntime(codeInvariants, registry);
    }
}
