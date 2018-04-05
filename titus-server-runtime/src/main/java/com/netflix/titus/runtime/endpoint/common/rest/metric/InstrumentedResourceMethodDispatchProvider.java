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

package com.netflix.titus.runtime.endpoint.common.rest.metric;

import java.util.Arrays;
import java.util.List;

import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import com.netflix.titus.runtime.endpoint.common.ClientInvocationMetrics;
import com.netflix.titus.runtime.endpoint.common.rest.RestServerConfiguration;
import com.netflix.titus.runtime.endpoint.resolver.HostCallerIdResolver;
import com.sun.jersey.api.model.AbstractResourceMethod;
import com.sun.jersey.spi.container.ResourceMethodDispatchProvider;
import com.sun.jersey.spi.dispatch.RequestDispatcher;

public class InstrumentedResourceMethodDispatchProvider implements ResourceMethodDispatchProvider {

    private static final String METRIC_ROOT = "titus.rest.resource.";

    private final RestServerConfiguration config;
    private final Registry registry;
    private final ResourceMethodDispatchProvider provider;
    private ClientInvocationMetrics clientInvocationMetrics;

    public InstrumentedResourceMethodDispatchProvider(RestServerConfiguration config,
                                                      HostCallerIdResolver hostCallerIdResolver,
                                                      Registry registry,
                                                      ResourceMethodDispatchProvider provider) {
        this.config = config;
        this.registry = registry;
        this.provider = provider;
        this.clientInvocationMetrics = new ClientInvocationMetrics(METRIC_ROOT, hostCallerIdResolver, registry);
    }

    @Override
    public RequestDispatcher create(AbstractResourceMethod method) {
        RequestDispatcher dispatcher = provider.create(method);
        if (dispatcher == null) {
            return null;
        }

        String resourceName = method.getDeclaringResource().getResourceClass().getSimpleName();
        String methodName = method.getMethod().getName();
        List<Tag> tags = Arrays.asList(new BasicTag("resource", resourceName), new BasicTag("method", methodName));

        return new InstrumentedRequestDispatcher(dispatcher, config, clientInvocationMetrics, tags, registry);
    }
}

