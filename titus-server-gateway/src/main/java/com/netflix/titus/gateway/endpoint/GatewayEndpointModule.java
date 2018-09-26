package com.netflix.titus.gateway.endpoint;

import com.google.inject.AbstractModule;
import com.netflix.governator.guice.jersey.GovernatorJerseySupportModule;
import com.netflix.titus.common.network.reverseproxy.http.ReactorHttpClientFactory;
import com.netflix.titus.gateway.endpoint.v3.ConfigurableReactorHttpClientFactory;
import com.netflix.titus.runtime.endpoint.resolver.HostCallerIdResolver;
import com.netflix.titus.runtime.endpoint.resolver.NoOpHostCallerIdResolver;

public class GatewayEndpointModule extends AbstractModule {

    private final boolean enableREST;

    public GatewayEndpointModule(boolean enableREST) {
        this.enableREST = enableREST;
    }

    @Override
    protected void configure() {
        bind(HostCallerIdResolver.class).to(NoOpHostCallerIdResolver.class);

        if (enableREST) {
            install(new GovernatorJerseySupportModule());
            install(new JerseyModule());

            bind(ReactorHttpClientFactory.class).to(ConfigurableReactorHttpClientFactory.class);
        }

        install(new GrpcModule());
    }
}
