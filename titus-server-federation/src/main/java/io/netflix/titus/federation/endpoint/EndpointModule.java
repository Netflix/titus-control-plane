package io.netflix.titus.federation.endpoint;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import io.netflix.titus.federation.endpoint.grpc.GrpcModule;
import io.netflix.titus.federation.endpoint.rest.JerseyModule;

public class EndpointModule extends AbstractModule {
    @Override
    protected void configure() {
        install(new GrpcModule());
        install(new JerseyModule());
    }

    @Provides
    @Singleton
    public EndpointConfiguration getGrpcEndpointConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(EndpointConfiguration.class);
    }
}
