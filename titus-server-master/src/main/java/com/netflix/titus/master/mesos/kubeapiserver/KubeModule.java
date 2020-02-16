/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.master.mesos.kubeapiserver;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.titus.api.FeatureActivationConfiguration;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.master.mesos.MesosConfiguration;
import com.netflix.titus.master.mesos.VirtualMachineMasterService;
import com.netflix.titus.master.mesos.kubeapiserver.direct.DefaultDirectKubeApiServerIntegrator;
import com.netflix.titus.master.mesos.kubeapiserver.direct.DefaultKubeApiFacade;
import com.netflix.titus.master.mesos.kubeapiserver.direct.DefaultTaskToPodConverter;
import com.netflix.titus.master.mesos.kubeapiserver.direct.DirectKubeApiServerIntegrator;
import com.netflix.titus.master.mesos.kubeapiserver.direct.DirectKubeConfiguration;
import com.netflix.titus.master.mesos.kubeapiserver.direct.KubeApiFacade;
import com.netflix.titus.master.mesos.kubeapiserver.direct.NoOpDirectKubeApiServerIntegrator;
import com.netflix.titus.master.mesos.kubeapiserver.direct.TaskToPodConverter;
import io.kubernetes.client.ApiClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubeModule extends AbstractModule {

    private static final Logger logger = LoggerFactory.getLogger(KubeModule.class);

    public static final String MESOS_KUBE_ADAPTER = "mesosKubeAdapter";

    @Override
    protected void configure() {
        bind(TaskToPodConverter.class).to(DefaultTaskToPodConverter.class);
        bind(KubeApiFacade.class).to(DefaultKubeApiFacade.class);
        bind(VirtualMachineMasterService.class).annotatedWith(Names.named(MESOS_KUBE_ADAPTER)).to(KubeApiServerIntegrator.class);
    }

    @Provides
    @Singleton
    public DirectKubeConfiguration getDirectKubeConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(DirectKubeConfiguration.class);
    }

    @Provides
    @Singleton
    public ApiClient getKubeApiClient(MesosConfiguration configuration, TitusRuntime titusRuntime) {
        return KubeUtil.createApiClient(
                configuration.getKubeApiServerUrl(),
                KubeApiServerIntegrator.CLIENT_METRICS_PREFIX,
                titusRuntime,
                0L
        );
    }

    @Provides
    @Singleton
    public DirectKubeApiServerIntegrator getDirectKubeApiServerIntegrator(FeatureActivationConfiguration configuration,
                                                                          Injector injector) {
        if (configuration.isKubeSchedulerEnabled()) {
            logger.info("Kube-scheduler enabled: starting DefaultDirectKubeApiServerIntegrator...");
            return injector.getInstance(DefaultDirectKubeApiServerIntegrator.class);
        }
        logger.info("Kube-scheduler disabled: starting NoOpDirectKubeApiServerIntegrator...");
        return new NoOpDirectKubeApiServerIntegrator();
    }
}
