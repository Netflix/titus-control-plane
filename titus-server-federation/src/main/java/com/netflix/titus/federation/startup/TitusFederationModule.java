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

package com.netflix.titus.federation.startup;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.governator.guice.jersey.GovernatorJerseySupportModule;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.federation.service.CellRouter;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.internal.DefaultTitusRuntime;
import com.netflix.titus.common.util.archaius2.Archaius2ConfigurationLogger;
import com.netflix.titus.common.util.code.CodeInvariants;
import com.netflix.titus.common.util.code.CompositeCodeInvariants;
import com.netflix.titus.common.util.code.LoggingCodeInvariants;
import com.netflix.titus.common.util.code.SpectatorCodeInvariants;
import com.netflix.titus.common.util.guice.ContainerEventBusModule;
import com.netflix.titus.federation.endpoint.EndpointModule;
import com.netflix.titus.federation.service.CellConnector;
import com.netflix.titus.federation.service.CellInfoResolver;
import com.netflix.titus.federation.service.DefaultCellConnector;
import com.netflix.titus.federation.service.DefaultCellInfoResolver;
import com.netflix.titus.federation.service.DefaultCellRouter;
import com.netflix.titus.federation.service.ServiceModule;
import com.netflix.titus.runtime.TitusEntitySanitizerModule;
import com.netflix.titus.runtime.endpoint.resolver.HostCallerIdResolver;
import com.netflix.titus.runtime.endpoint.resolver.NoOpHostCallerIdResolver;

public class TitusFederationModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(Archaius2ConfigurationLogger.class).asEagerSingleton();
        bind(Registry.class).toInstance(new DefaultRegistry());

        install(new GovernatorJerseySupportModule());

        install(new ContainerEventBusModule());
        install(new TitusEntitySanitizerModule());

        bind(HostCallerIdResolver.class).to(NoOpHostCallerIdResolver.class);
        bind(CellConnector.class).to(DefaultCellConnector.class);
        bind(CellInfoResolver.class).to(DefaultCellInfoResolver.class);
        bind(CellRouter.class).to(DefaultCellRouter.class);

        install(new EndpointModule());
        install(new ServiceModule());
    }

    @Provides
    @Singleton
    public TitusFederationConfiguration getConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(TitusFederationConfiguration.class);
    }

    @Provides
    @Singleton
    public TitusRuntime getTitusRuntime(Registry registry) {
        CodeInvariants codeInvariants = new CompositeCodeInvariants(
                LoggingCodeInvariants.INSTANCE,
                new SpectatorCodeInvariants(registry.createId("titus.runtime.invariant.violations"), registry)
        );
        return new DefaultTitusRuntime(codeInvariants, registry);
    }

    @Provides
    @Singleton
    public GrpcConfiguration getGrpcConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(GrpcConfiguration.class);
    }
}
