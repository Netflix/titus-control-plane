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

package com.netflix.titus.runtime.endpoint.common.rest.provider;

import javax.inject.Inject;
import javax.ws.rs.ext.Provider;

import com.netflix.spectator.api.Registry;
import com.netflix.titus.runtime.endpoint.common.rest.RestServerConfiguration;
import com.netflix.titus.runtime.endpoint.common.rest.metric.InstrumentedResourceMethodDispatchProvider;
import com.netflix.titus.runtime.endpoint.resolver.HostCallerIdResolver;
import com.sun.jersey.spi.container.ResourceMethodDispatchAdapter;
import com.sun.jersey.spi.container.ResourceMethodDispatchProvider;

@Provider
public class InstrumentedResourceMethodDispatchAdapter implements ResourceMethodDispatchAdapter {

    private final RestServerConfiguration config;
    private final Registry registry;
    private final HostCallerIdResolver hostCallerIdResolver;

    @Inject
    public InstrumentedResourceMethodDispatchAdapter(RestServerConfiguration config, HostCallerIdResolver hostCallerIdResolver, Registry registry) {
        this.config = config;
        this.registry = registry;
        this.hostCallerIdResolver = hostCallerIdResolver;
    }

    @Override
    public ResourceMethodDispatchProvider adapt(ResourceMethodDispatchProvider provider) {
        return new InstrumentedResourceMethodDispatchProvider(config, hostCallerIdResolver, registry, provider);
    }
}
