/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.endpoint.v2.rest.metric;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import com.sun.jersey.api.model.AbstractResourceMethod;
import com.sun.jersey.spi.container.ResourceMethodDispatchProvider;
import com.sun.jersey.spi.dispatch.RequestDispatcher;
import io.netflix.titus.master.MetricConstants;
import io.netflix.titus.master.endpoint.v2.rest.RestConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InstrumentedResourceMethodDispatchProvider implements ResourceMethodDispatchProvider {

    private static final Logger logger = LoggerFactory.getLogger(InstrumentedResourceMethodDispatchProvider.class);

    private final RestConfig config;
    private final Registry registry;
    private final ResourceMethodDispatchProvider provider;

    public InstrumentedResourceMethodDispatchProvider(RestConfig config, Registry registry, ResourceMethodDispatchProvider provider) {
        this.config = config;
        this.registry = registry;
        this.provider = provider;
    }

    @Override
    public RequestDispatcher create(AbstractResourceMethod method) {
        RequestDispatcher dispatcher = provider.create(method);
        if (dispatcher == null) {
            return null;
        }

        String resourceName = method.getDeclaringResource().getResourceClass().getSimpleName();
        String methodName = method.getMethod().getName();
        String[] tags = {"resource", resourceName, "method", methodName};

        Counter successCounter = registry.counter(MetricConstants.METRIC_REST + "successCount", tags);
        Counter failureCounter = registry.counter(MetricConstants.METRIC_REST + "failureCount", tags);
        Timer latencyTimer = registry.timer(MetricConstants.METRIC_REST + "latency", tags);

        return new InstrumentedRequestDispatcher(dispatcher, config, registry, successCounter, failureCounter, latencyTimer);
    }
}

