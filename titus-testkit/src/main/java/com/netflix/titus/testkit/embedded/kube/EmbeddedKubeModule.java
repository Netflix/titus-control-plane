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

package com.netflix.titus.testkit.embedded.kube;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.titus.master.jobmanager.service.ComputeProvider;
import com.netflix.titus.master.kubernetes.client.DefaultDirectKubeApiServerIntegrator;
import com.netflix.titus.master.kubernetes.client.DirectKubeApiServerIntegrator;
import com.netflix.titus.master.kubernetes.client.DirectKubeConfiguration;
import com.netflix.titus.runtime.connector.kubernetes.KubeApiFacade;

public class EmbeddedKubeModule extends AbstractModule {

    private final EmbeddedKubeCluster embeddedKubeCluster;

    public EmbeddedKubeModule(EmbeddedKubeCluster embeddedKubeCluster) {
        this.embeddedKubeCluster = embeddedKubeCluster;
    }

    @Override
    protected void configure() {
        bind(KubeApiFacade.class).toInstance(embeddedKubeCluster.getKubeApiFacade());
        bind(DirectKubeApiServerIntegrator.class).to(DefaultDirectKubeApiServerIntegrator.class);
    }

    @Provides
    @Singleton
    public DirectKubeConfiguration getDirectKubeConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(DirectKubeConfiguration.class);
    }

    @Provides
    @Singleton
    public ComputeProvider getComputeProvider(DirectKubeApiServerIntegrator integrator) {
        return integrator;
    }
}
