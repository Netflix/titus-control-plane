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

package io.netflix.titus.federation.endpoint.rest;

import java.util.function.UnaryOperator;
import javax.inject.Named;
import javax.inject.Singleton;

import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.governator.guice.jersey.GovernatorServletContainer;
import com.netflix.governator.providers.Advises;
import com.sun.jersey.api.core.DefaultResourceConfig;
import com.sun.jersey.guice.JerseyServletModule;
import io.netflix.titus.runtime.endpoint.common.rest.JsonMessageReaderWriter;
import io.netflix.titus.runtime.endpoint.common.rest.RestServerConfiguration;
import io.netflix.titus.runtime.endpoint.common.rest.TitusExceptionMapper;
import io.netflix.titus.runtime.endpoint.common.rest.filter.CallerContextFilter;
import io.netflix.titus.runtime.endpoint.common.rest.provider.InstrumentedResourceMethodDispatchAdapter;

/**
 * We use this module to wire up our endpoints.
 */
public final class JerseyModule extends JerseyServletModule {
    @Override
    protected void configureServlets() {
        // Store HTTP servlet request data in thread local variable
        filter("/api/v3/*").through(CallerContextFilter.class);

        // Configure servlet to serve resources for all other api paths
        serve("/api/*").with(GovernatorServletContainer.class);
    }

    @Provides
    @Singleton
    public RestServerConfiguration getRestServerConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(RestServerConfiguration.class);
    }

    @Advises
    @Singleton
    @Named("governator")
    UnaryOperator<DefaultResourceConfig> getConfig() {
        return config -> {
            // providers
            config.getClasses().add(JsonMessageReaderWriter.class);
            config.getClasses().add(TitusExceptionMapper.class);
            config.getClasses().add(InstrumentedResourceMethodDispatchAdapter.class);

            // resources
            config.getClasses().add(JobManagementResource.class);
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
