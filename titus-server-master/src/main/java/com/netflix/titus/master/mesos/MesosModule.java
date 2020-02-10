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

package com.netflix.titus.master.mesos;

import java.time.Duration;
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.runtime.health.guice.HealthModule;
import com.netflix.titus.api.FeatureActivationConfiguration;
import com.netflix.titus.common.framework.scheduler.LocalScheduler;
import com.netflix.titus.common.framework.scheduler.internal.DefaultLocalScheduler;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.master.mesos.kubeapiserver.KubeApiServerIntegrator;
import com.netflix.titus.master.mesos.kubeapiserver.KubeOpportunisticResourceProvider;
import com.netflix.titus.master.mesos.kubeapiserver.KubeUtil;
import com.netflix.titus.master.mesos.kubeapiserver.direct.DefaultDirectKubeApiServerIntegrator;
import com.netflix.titus.master.mesos.kubeapiserver.direct.DefaultTaskToPodConverter;
import com.netflix.titus.master.mesos.kubeapiserver.direct.DirectKubeApiServerIntegrator;
import com.netflix.titus.master.mesos.kubeapiserver.direct.DirectKubeConfiguration;
import com.netflix.titus.master.mesos.kubeapiserver.direct.NoOpDirectKubeApiServerIntegrator;
import com.netflix.titus.master.mesos.kubeapiserver.direct.TaskToPodConverter;
import com.netflix.titus.master.mesos.resolver.DefaultMesosMasterResolver;
import com.netflix.titus.master.scheduler.opportunistic.NoOpportunisticCpus;
import com.netflix.titus.master.scheduler.opportunistic.OpportunisticCpuAvailabilityProvider;
import io.kubernetes.client.ApiClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Schedulers;

public class MesosModule extends AbstractModule {

    private static final Logger logger = LoggerFactory.getLogger(MesosModule.class);

    @Override
    protected void configure() {
        bind(MesosMasterResolver.class).to(DefaultMesosMasterResolver.class);
        bind(MesosSchedulerDriverFactory.class).to(StdSchedulerDriverFactory.class);
        bind(VirtualMachineMasterServiceActivator.class).asEagerSingleton();

        bind(TaskToPodConverter.class).to(DefaultTaskToPodConverter.class);

        bind(WorkerStateMonitor.class).asEagerSingleton();

        install(new HealthModule() {
            @Override
            protected void configureHealth() {
                bindAdditionalHealthIndicator().to(MesosHealthIndicator.class);
            }
        });
    }

    @Provides
    @Singleton
    public MesosConfiguration getMesosConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(MesosConfiguration.class);
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
    public LocalScheduler getLocalScheduler(TitusRuntime titusRuntime) {
        return new DefaultLocalScheduler(Duration.ofMillis(100), Schedulers.elastic(), titusRuntime.getClock(), titusRuntime.getRegistry());
    }

    @Provides
    @Singleton
    public VirtualMachineMasterService getVirtualMachineMasterService(MesosConfiguration configuration,
                                                                      Injector injector) {
        if (configuration.isKubeApiServerIntegrationEnabled()) {
            return injector.getInstance(KubeApiServerIntegrator.class);
        }
        return injector.getInstance(VirtualMachineMasterServiceMesosImpl.class);
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

    @Provides
    @Singleton
    public OpportunisticCpuAvailabilityProvider getOpportunisticCpusAvailabilityProvider(MesosConfiguration configuration,
                                                                                         Injector injector) {
        if (configuration.isKubeApiServerIntegrationEnabled()) {
            return injector.getInstance(KubeOpportunisticResourceProvider.class);
        }
        return injector.getInstance(NoOpportunisticCpus.class);
    }
}
