/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.master.kubernetes.client;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.master.mesos.MesosConfiguration;
import com.netflix.titus.runtime.connector.kubernetes.KubeApiClients;
import com.netflix.titus.runtime.connector.kubernetes.KubeApiFacade;
import com.netflix.titus.runtime.connector.kubernetes.NoOpKubeApiFacade;
import io.kubernetes.client.openapi.ApiClient;

public class KubeClientModule extends AbstractModule {

    public static final String CLIENT_METRICS_PREFIX = "titusMaster.mesos.kubeApiServerIntegration";

    @Override
    protected void configure() {
        bind(DirectKubeApiServerIntegrator.class).to(DefaultDirectKubeApiServerIntegrator.class);
    }

    @Provides
    @Singleton
    public DirectKubeConfiguration getDirectKubeConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(DirectKubeConfiguration.class);
    }

    @Provides
    @Singleton
    public ApiClient getKubeApiClient(MesosConfiguration configuration, TitusRuntime titusRuntime) {
        return KubeApiClients.createApiClient(
                configuration.getKubeApiServerUrl(),
                configuration.getKubeConfigPath(),
                CLIENT_METRICS_PREFIX,
                titusRuntime,
                0L,
                configuration.isCompressionEnabledForKubeApiClient()
        );
    }

    @Provides
    @Singleton
    public KubeApiFacade getKubeApiFacade(MesosConfiguration mesosConfiguration, Injector injector) {
        if (mesosConfiguration.isKubeApiServerIntegrationEnabled()) {
            return injector.getInstance(JobControllerKubeApiFacade.class);
        }
        return new NoOpKubeApiFacade();
    }
}
