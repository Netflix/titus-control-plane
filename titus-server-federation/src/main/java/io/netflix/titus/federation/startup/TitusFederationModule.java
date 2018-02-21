package io.netflix.titus.federation.startup;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.governator.guice.jersey.GovernatorJerseySupportModule;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import io.netflix.titus.common.runtime.TitusRuntime;
import io.netflix.titus.common.runtime.internal.DefaultTitusRuntime;
import io.netflix.titus.common.util.archaius2.Archaius2ConfigurationLogger;
import io.netflix.titus.common.util.guice.ContainerEventBusModule;
import io.netflix.titus.federation.endpoint.EndpointModule;
import io.netflix.titus.federation.jobmanager.JobManagerModule;
import io.netflix.titus.runtime.TitusEntitySanitizerModule;
import io.netflix.titus.runtime.endpoint.resolver.HostCallerIdResolver;
import io.netflix.titus.runtime.endpoint.resolver.NoOpHostCallerIdResolver;

public class TitusFederationModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(Archaius2ConfigurationLogger.class).asEagerSingleton();
        bind(Registry.class).toInstance(new DefaultRegistry());
        bind(TitusRuntime.class).to(DefaultTitusRuntime.class);

        install(new GovernatorJerseySupportModule());

        install(new ContainerEventBusModule());
        install(new TitusEntitySanitizerModule());

        bind(HostCallerIdResolver.class).to(NoOpHostCallerIdResolver.class);

        install(new EndpointModule());
        install(new JobManagerModule());
    }

    @Provides
    @Singleton
    public TitusFederationConfiguration getConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(TitusFederationConfiguration.class);
    }
}
