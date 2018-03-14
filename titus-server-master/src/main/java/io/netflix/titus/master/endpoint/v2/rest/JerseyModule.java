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

package io.netflix.titus.master.endpoint.v2.rest;

import java.util.function.UnaryOperator;
import javax.inject.Named;
import javax.inject.Singleton;

import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.governator.guice.jersey.GovernatorServletContainer;
import com.netflix.governator.providers.Advises;
import com.sun.jersey.api.core.DefaultResourceConfig;
import com.sun.jersey.guice.JerseyServletModule;
import io.netflix.titus.api.store.v2.V2WorkerMetadata;
import io.netflix.titus.master.config.MasterConfiguration;
import io.netflix.titus.master.endpoint.v2.rest.filter.LeaderRedirectingFilter;
import io.netflix.titus.runtime.endpoint.common.LogStorageInfo;
import io.netflix.titus.runtime.endpoint.common.rest.JsonMessageReaderWriter;
import io.netflix.titus.runtime.endpoint.common.rest.RestServerConfiguration;
import io.netflix.titus.runtime.endpoint.common.rest.TitusExceptionMapper;
import io.netflix.titus.runtime.endpoint.common.rest.metric.ResettableInputStreamFilter;
import io.netflix.titus.runtime.endpoint.common.rest.provider.InstrumentedResourceMethodDispatchAdapter;
import io.netflix.titus.runtime.endpoint.fit.FitResource;

/**
 * We use this module to wire up our endpoints.
 */
public final class JerseyModule extends JerseyServletModule {

    public static final TypeLiteral<LogStorageInfo<V2WorkerMetadata>> V2_LOG_STORAGE_INFO =
            new TypeLiteral<LogStorageInfo<V2WorkerMetadata>>() {
            };

    @Override
    protected void configureServlets() {
        filter("/*").through(LeaderRedirectingFilter.class);
        filter("/*").through(ResettableInputStreamFilter.class);

        // This sets up Jersey to serve any found resources that start with the base path of "/*"
        serve("/*").with(GovernatorServletContainer.class);
    }

    @Provides
    @Singleton
    public RestConfig getRestConfig(ConfigProxyFactory factory) {
        return factory.newProxy(RestConfig.class);
    }

    @Provides
    @Singleton
    public RestServerConfiguration getRestServerConfiguration(RestConfig restConfig) {
        return restConfig;
    }

    @Advises
    @Singleton
    @Named("governator")
    UnaryOperator<DefaultResourceConfig> getConfig(MasterConfiguration configuration) {
        return config -> {
            // Providers
            config.getClasses().add(JsonMessageReaderWriter.class);
            config.getClasses().add(TitusExceptionMapper.class);
            config.getClasses().add(InstrumentedResourceMethodDispatchAdapter.class);

            // Runtime resources
            config.getClasses().add(HealthCheckResource.class);
            config.getClasses().add(LeaderResource.class);
            config.getClasses().add(FitResource.class);
            config.getClasses().add(ServerStatusResource.class);

            // V2 resources
            config.getClasses().add(SchedulerResource.class);
            config.getClasses().add(VmManagementResource.class);
            config.getClasses().add(LogsResource.class);
            config.getClasses().add(ApplicationSlaManagementResource.class);
            config.getClasses().add(ResourceConsumptionResource.class);

            if (configuration.isV2Enabled()) {
                config.getClasses().add(JobManagementResource.class);
            }
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
