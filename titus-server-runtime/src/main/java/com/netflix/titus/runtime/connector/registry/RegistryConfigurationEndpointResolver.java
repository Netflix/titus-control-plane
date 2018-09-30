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

package com.netflix.titus.runtime.connector.registry;

import java.net.URI;
import java.net.URISyntaxException;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.common.network.client.RxRestClientException;
import rx.Observable;

/**
 * This {@link RegistryEndpointResolver} implementation resolves a Registry endpoint based on
 * * a provided configuration.
 */
@Singleton
public class RegistryConfigurationEndpointResolver implements RegistryEndpointResolver {

    private final TitusRegistryClientConfiguration titusRegistryClientConfiguration;

    @Inject
    public RegistryConfigurationEndpointResolver(TitusRegistryClientConfiguration configuration) {
        this.titusRegistryClientConfiguration = configuration;
    }

    @Override
    public Observable<URI> resolve() {
        String uriStr = "https://" + titusRegistryClientConfiguration.getRegistryHostname() + ":" + titusRegistryClientConfiguration.getRegistryHttpPort();
        try {
            return Observable.just(new URI(uriStr));
        } catch (URISyntaxException e) {
            return Observable.error(new RxRestClientException("Invalid URI: " + uriStr));
        }
    }
}
