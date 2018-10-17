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

package com.netflix.titus.supplementary.relocation.endpoint.rest;

import java.util.function.UnaryOperator;
import javax.inject.Named;
import javax.inject.Singleton;

import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.governator.guice.jersey.GovernatorServletContainer;
import com.netflix.governator.providers.Advises;
import com.netflix.titus.common.framework.scheduler.endpoint.LocalSchedulerResource;
import com.netflix.titus.runtime.endpoint.common.rest.JsonMessageReaderWriter;
import com.netflix.titus.runtime.endpoint.common.rest.RestServerConfiguration;
import com.netflix.titus.runtime.endpoint.common.rest.TitusExceptionMapper;
import com.netflix.titus.runtime.endpoint.common.rest.metric.ResettableInputStreamFilter;
import com.netflix.titus.runtime.endpoint.common.rest.provider.InstrumentedResourceMethodDispatchAdapter;
import com.netflix.titus.runtime.endpoint.metadata.SimpleHttpCallMetadataResolver;
import com.sun.jersey.api.core.DefaultResourceConfig;
import com.sun.jersey.guice.JerseyServletModule;

public class TaskRelocationJerseyModule extends JerseyServletModule {

    @Override
    protected void configureServlets() {
        // Call metadata interceptor (see CallMetadataHeaders).
        filter("/api/v3/*").through(SimpleHttpCallMetadataResolver.CallMetadataInterceptorFilter.class);

        filter("/*").through(ResettableInputStreamFilter.class);

        // This sets up Jersey to serve any found resources that start with the base path of "/*"
        serve("/*").with(GovernatorServletContainer.class);
    }

    @Provides
    @Singleton
    public RestServerConfiguration getRestServerConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(RestServerConfiguration.class);
    }

    @Advises
    @Singleton
    @Named("governator")
    public UnaryOperator<DefaultResourceConfig> getConfig() {
        return config -> {
            // Providers
            config.getClasses().add(JsonMessageReaderWriter.class);
            config.getClasses().add(TitusExceptionMapper.class);
            config.getClasses().add(InstrumentedResourceMethodDispatchAdapter.class);

            // Runtime resources
            config.getClasses().add(LocalSchedulerResource.class);
            config.getClasses().add(TaskRelocationResource.class);

            return config;
        };
    }

    @Override
    public boolean equals(Object obj) {
        return obj != null && getClass().equals(obj.getClass());
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }
}