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

import java.util.Arrays;
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.archaius.api.Config;
import com.netflix.governator.guice.jersey.GovernatorJerseySupportModule;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.api.model.callmetadata.CallMetadataConstants;
import com.netflix.titus.common.environment.MyEnvironments;
import com.netflix.titus.common.runtime.SystemAbortListener;
import com.netflix.titus.common.runtime.SystemLogService;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.internal.DefaultTitusRuntime;
import com.netflix.titus.common.runtime.internal.LoggingSystemAbortListener;
import com.netflix.titus.common.runtime.internal.LoggingSystemLogService;
import com.netflix.titus.common.util.archaius2.Archaius2ConfigurationLogger;
import com.netflix.titus.common.util.code.CodeInvariants;
import com.netflix.titus.common.util.code.CompositeCodeInvariants;
import com.netflix.titus.common.util.code.LoggingCodeInvariants;
import com.netflix.titus.common.util.code.SpectatorCodeInvariants;
import com.netflix.titus.common.util.grpc.reactor.GrpcToReactorServerFactory;
import com.netflix.titus.common.util.grpc.reactor.server.DefaultGrpcToReactorServerFactory;
import com.netflix.titus.common.util.guice.ContainerEventBusModule;
import com.netflix.titus.federation.endpoint.FederationEndpointModule;
import com.netflix.titus.federation.service.CellConnector;
import com.netflix.titus.federation.service.CellInfoResolver;
import com.netflix.titus.federation.service.CellWebClientConnector;
import com.netflix.titus.federation.service.DefaultCellConnector;
import com.netflix.titus.federation.service.DefaultCellInfoResolver;
import com.netflix.titus.federation.service.DefaultCellWebClientConnector;
import com.netflix.titus.federation.service.DefaultRemoteFederationConnector;
import com.netflix.titus.federation.service.DefaultRemoteFederationInfoResolver;
import com.netflix.titus.federation.service.RemoteFederationConnector;
import com.netflix.titus.federation.service.RemoteFederationInfoResolver;
import com.netflix.titus.federation.service.ServiceModule;
import com.netflix.titus.federation.service.SimpleWebClientFactory;
import com.netflix.titus.federation.service.WebClientFactory;
import com.netflix.titus.federation.service.router.ApplicationCellRouter;
import com.netflix.titus.federation.service.router.CellRouter;
import com.netflix.titus.federation.service.router.ChainCellRouter;
import com.netflix.titus.federation.service.router.FallbackCellRouter;
import com.netflix.titus.federation.service.router.SpecialInstanceTypeRouter;
import com.netflix.titus.runtime.TitusEntitySanitizerModule;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.endpoint.resolver.HostCallerIdResolver;
import com.netflix.titus.runtime.endpoint.resolver.NoOpHostCallerIdResolver;

public class TitusFederationModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(Archaius2ConfigurationLogger.class).asEagerSingleton();
        bind(Registry.class).toInstance(new DefaultRegistry());
        bind(SystemLogService.class).to(LoggingSystemLogService.class);
        bind(SystemAbortListener.class).to(LoggingSystemAbortListener.class);

        install(new GovernatorJerseySupportModule());

        install(new ContainerEventBusModule());
        install(new TitusEntitySanitizerModule());

        bind(HostCallerIdResolver.class).to(NoOpHostCallerIdResolver.class);
        bind(CellConnector.class).to(DefaultCellConnector.class);
        bind(RemoteFederationConnector.class).to(DefaultRemoteFederationConnector.class);
        bind(CellWebClientConnector.class).to(DefaultCellWebClientConnector.class);
        bind(WebClientFactory.class).toInstance(SimpleWebClientFactory.getInstance());
        bind(CellInfoResolver.class).to(DefaultCellInfoResolver.class);
        bind(RemoteFederationInfoResolver.class).to(DefaultRemoteFederationInfoResolver.class);

        install(new FederationEndpointModule());
        install(new ServiceModule());
    }

    @Provides
    @Singleton
    public TitusFederationConfiguration getConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(TitusFederationConfiguration.class);
    }

    @Provides
    @Singleton
    public TitusRuntime getTitusRuntime(Config config, SystemLogService systemLogService, SystemAbortListener systemAbortListener, Registry registry) {
        CodeInvariants codeInvariants = new CompositeCodeInvariants(
                LoggingCodeInvariants.getDefault(),
                new SpectatorCodeInvariants(registry.createId("titus.runtime.invariant.violations"), registry)
        );
        return new DefaultTitusRuntime(MyEnvironments.newArchaius(config), codeInvariants, systemLogService, false, systemAbortListener, registry);
    }

    @Provides
    @Singleton
    public GrpcConfiguration getGrpcConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(GrpcConfiguration.class);
    }

    @Provides
    @Singleton
    public GrpcToReactorServerFactory getGrpcToReactorServerFactory(CallMetadataResolver callMetadataResolver) {
        return new DefaultGrpcToReactorServerFactory<>(
                CallMetadata.class,
                () -> callMetadataResolver.resolve().orElse(CallMetadataConstants.UNDEFINED_CALL_METADATA)
        );
    }

    @Provides
    @Singleton
    public CellRouter getCellRouter(CellInfoResolver cellInfoResolver, TitusFederationConfiguration federationConfiguration) {
        return new ChainCellRouter(Arrays.asList(
                SpecialInstanceTypeRouter.getGpuInstanceTypeRouter(cellInfoResolver, federationConfiguration),
                new ApplicationCellRouter(cellInfoResolver, federationConfiguration),
                new FallbackCellRouter(cellInfoResolver)
        ));
    }
}
