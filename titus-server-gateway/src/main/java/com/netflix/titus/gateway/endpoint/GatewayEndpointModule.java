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

package com.netflix.titus.gateway.endpoint;

import com.google.inject.AbstractModule;
import com.netflix.governator.guice.jersey.GovernatorJerseySupportModule;
import com.netflix.titus.common.network.reverseproxy.http.ReactorHttpClientFactory;
import com.netflix.titus.gateway.endpoint.v3.ConfigurableReactorHttpClientFactory;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolveModule;
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

        install(new CallMetadataResolveModule());

        if (enableREST) {
            install(new GovernatorJerseySupportModule());
            install(new GatewayJerseyModule());

            bind(ReactorHttpClientFactory.class).to(ConfigurableReactorHttpClientFactory.class);
        }

        install(new GatewayGrpcModule());
    }
}
