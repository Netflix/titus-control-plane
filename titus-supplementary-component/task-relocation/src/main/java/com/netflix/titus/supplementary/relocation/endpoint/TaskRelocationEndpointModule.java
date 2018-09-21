package com.netflix.titus.supplementary.relocation.endpoint;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.governator.guice.jersey.GovernatorJerseySupportModule;
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
    public TaskRelocationEndpointConfiguration getTaskRelocationEndpointConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(TaskRelocationEndpointConfiguration.class);
    }
}
