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
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.runtime.health.guice.HealthModule;
import com.netflix.titus.common.framework.scheduler.LocalScheduler;
import com.netflix.titus.common.framework.scheduler.internal.DefaultLocalScheduler;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.master.mesos.kubeapiserver.KubeOpportunisticResourceProvider;
import com.netflix.titus.master.mesos.kubeapiserver.LegacyKubeModule;
import com.netflix.titus.master.mesos.resolver.DefaultMesosMasterResolver;
import com.netflix.titus.master.scheduler.opportunistic.NoOpportunisticCpus;
import com.netflix.titus.master.scheduler.opportunistic.OpportunisticCpuAvailabilityProvider;
import reactor.core.scheduler.Schedulers;

import static com.netflix.titus.master.mesos.kubeapiserver.LegacyKubeModule.MESOS_KUBE_ADAPTER;

public class MesosModule extends AbstractModule {

    public static final String MESOS_INTEGRATION = "mesosIntegration";

    @Override
    protected void configure() {
        bind(MesosMasterResolver.class).to(DefaultMesosMasterResolver.class);
        bind(MesosSchedulerDriverFactory.class).to(StdSchedulerDriverFactory.class);
        bind(VirtualMachineMasterServiceActivator.class).asEagerSingleton();

        bind(VirtualMachineMasterService.class).annotatedWith(Names.named(MESOS_INTEGRATION)).to(VirtualMachineMasterServiceMesosImpl.class);

        bind(WorkerStateMonitor.class).asEagerSingleton();

        install(new HealthModule() {
            @Override
            protected void configureHealth() {
                bindAdditionalHealthIndicator().to(MesosHealthIndicator.class);
            }
        });

        install(new LegacyKubeModule());
    }

    @Provides
    @Singleton
    public MesosConfiguration getMesosConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(MesosConfiguration.class);
    }

    @Provides
    @Singleton
    public LocalScheduler getLocalScheduler(TitusRuntime titusRuntime) {
        return new DefaultLocalScheduler(Duration.ofMillis(100), Schedulers.elastic(), titusRuntime.getClock(), titusRuntime.getRegistry());
    }

    @Provides
    @Singleton
    public VirtualMachineMasterService getVirtualMachineMasterService(Injector injector, MesosConfiguration configuration) {
        String annotationName = configuration.isKubeApiServerIntegrationEnabled() ? MESOS_KUBE_ADAPTER : MESOS_INTEGRATION;
        return injector.getInstance(Key.get(VirtualMachineMasterService.class, Names.named(annotationName)));
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
