package com.netflix.titus.ext.eureka.reverseproxy;

import com.google.inject.AbstractModule;
import com.netflix.titus.common.network.reverseproxy.http.ReactorHttpClientFactory;
import com.netflix.titus.ext.eureka.reverseproxy.http.EurekaReactorHttpClientFactory;

public class EurekaReverseProxyModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(ReactorHttpClientFactory.class).to(EurekaReactorHttpClientFactory.class);
    }
}
