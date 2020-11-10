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

import java.util.Arrays;
import javax.inject.Named;
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.archaius.api.Config;
import com.netflix.titus.api.FeatureActivationConfiguration;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.limiter.tokenbucket.FixedIntervalTokenBucketConfiguration;
import com.netflix.titus.master.kubernetes.controller.KubeControllerModule;
import com.netflix.titus.master.mesos.MesosConfiguration;
import com.netflix.titus.master.mesos.VirtualMachineMasterService;
import com.netflix.titus.master.mesos.kubeapiserver.client.JobControllerKubeApiFacade;
import com.netflix.titus.master.mesos.kubeapiserver.direct.DefaultDirectKubeApiServerIntegrator;
import com.netflix.titus.master.mesos.kubeapiserver.direct.DefaultPodAffinityFactory;
import com.netflix.titus.master.mesos.kubeapiserver.direct.DefaultTaskToPodConverter;
import com.netflix.titus.master.mesos.kubeapiserver.direct.DirectKubeApiServerIntegrator;
import com.netflix.titus.master.mesos.kubeapiserver.direct.DirectKubeConfiguration;
import com.netflix.titus.master.mesos.kubeapiserver.direct.NoOpDirectKubeApiServerIntegrator;
import com.netflix.titus.master.mesos.kubeapiserver.direct.PodAffinityFactory;
import com.netflix.titus.master.mesos.kubeapiserver.direct.TaskToPodConverter;
import com.netflix.titus.master.mesos.kubeapiserver.direct.env.ContainerEnvFactory;
import com.netflix.titus.master.mesos.kubeapiserver.direct.env.ContainerEnvs;
import com.netflix.titus.master.mesos.kubeapiserver.direct.resourcepool.CapacityGroupPodResourcePoolResolver;
import com.netflix.titus.master.mesos.kubeapiserver.direct.resourcepool.ExplicitJobPodResourcePoolResolver;
import com.netflix.titus.master.mesos.kubeapiserver.direct.resourcepool.FarzonePodResourcePoolResolver;
import com.netflix.titus.master.mesos.kubeapiserver.direct.resourcepool.GpuPodResourcePoolResolver;
import com.netflix.titus.master.mesos.kubeapiserver.direct.resourcepool.PodResourcePoolResolver;
import com.netflix.titus.master.mesos.kubeapiserver.direct.resourcepool.PodResourcePoolResolverChain;
import com.netflix.titus.master.mesos.kubeapiserver.direct.resourcepool.PodResourcePoolResolverFeatureGuard;
import com.netflix.titus.master.mesos.kubeapiserver.direct.resourcepool.TierPodResourcePoolResolver;
import com.netflix.titus.master.mesos.kubeapiserver.direct.taint.DefaultTaintTolerationFactory;
import com.netflix.titus.master.mesos.kubeapiserver.direct.taint.TaintTolerationFactory;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import com.netflix.titus.runtime.connector.kubernetes.KubeApiClients;
import com.netflix.titus.runtime.connector.kubernetes.KubeApiFacade;
import com.netflix.titus.runtime.connector.kubernetes.NoOpKubeApiFacade;
import io.kubernetes.client.openapi.ApiClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.master.mesos.kubeapiserver.DefaultKubeJobManagementReconciler.GC_UNKNOWN_PODS;

public class KubeModule extends AbstractModule {

    private static final Logger logger = LoggerFactory.getLogger(KubeModule.class);

    public static final String MESOS_KUBE_ADAPTER = "mesosKubeAdapter";

    private static final String RESOURCE_POOL_PROPERTIES_PREFIX = "titus.resourcePools";

    @Override
    protected void configure() {
        bind(ContainerEnvFactory.class).toInstance(ContainerEnvs.getDefaultFactory());
        bind(TaskToPodConverter.class).to(DefaultTaskToPodConverter.class);
        bind(ContainerResultCodeResolver.class).to(DefaultContainerResultCodeResolver.class);
        bind(PodAffinityFactory.class).to(DefaultPodAffinityFactory.class);
        bind(TaintTolerationFactory.class).to(DefaultTaintTolerationFactory.class);
        bind(VirtualMachineMasterService.class).annotatedWith(Names.named(MESOS_KUBE_ADAPTER)).to(KubeApiServerIntegrator.class);
        install(new KubeControllerModule());
    }

    @Provides
    @Singleton
    public DirectKubeConfiguration getDirectKubeConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(DirectKubeConfiguration.class);
    }

    @Provides
    @Singleton
    @Named(GC_UNKNOWN_PODS)
    public FixedIntervalTokenBucketConfiguration getGcUnknownPodsTokenBucketConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(FixedIntervalTokenBucketConfiguration.class, "titusMaster.kube.gcUnknownPodsTokenBucket");
    }

    @Provides
    @Singleton
    public PodResourcePoolResolver getPodResourcePoolResolver(DirectKubeConfiguration configuration,
                                                              Config config,
                                                              ApplicationSlaManagementService capacityGroupService,
                                                              TitusRuntime titusRuntime) {
        return new PodResourcePoolResolverFeatureGuard(
                configuration,
                new PodResourcePoolResolverChain(Arrays.asList(
                        new ExplicitJobPodResourcePoolResolver(),
                        new FarzonePodResourcePoolResolver(configuration),
                        new GpuPodResourcePoolResolver(configuration),
                        new CapacityGroupPodResourcePoolResolver(
                                configuration,
                                config.getPrefixedView(RESOURCE_POOL_PROPERTIES_PREFIX),
                                capacityGroupService,
                                titusRuntime
                        ),
                        new TierPodResourcePoolResolver(capacityGroupService)
                ))
        );
    }

    @Provides
    @Singleton
    public ApiClient getKubeApiClient(MesosConfiguration configuration, TitusRuntime titusRuntime) {
        return KubeApiClients.createApiClient(
                configuration.getKubeApiServerUrl(),
                configuration.getKubeConfigPath(),
                KubeApiServerIntegrator.CLIENT_METRICS_PREFIX,
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

    @Provides
    @Singleton
    public DirectKubeApiServerIntegrator getDirectKubeApiServerIntegrator(FeatureActivationConfiguration configuration,
                                                                          MesosConfiguration mesosConfiguration,
                                                                          Injector injector) {
        if (mesosConfiguration.isKubeApiServerIntegrationEnabled() && configuration.isKubeSchedulerEnabled()) {
            logger.info("Kube-scheduler enabled: starting DefaultDirectKubeApiServerIntegrator...");
            return injector.getInstance(DefaultDirectKubeApiServerIntegrator.class);
        }
        logger.info("Kube-scheduler disabled: starting NoOpDirectKubeApiServerIntegrator...");
        return new NoOpDirectKubeApiServerIntegrator();
    }

    @Provides
    @Singleton
    public KubeJobManagementReconciler getKubeJobManagementReconciler(MesosConfiguration mesosConfiguration,
                                                                      Injector injector) {
        if (mesosConfiguration.isKubeApiServerIntegrationEnabled()) {
            return injector.getInstance(DefaultKubeJobManagementReconciler.class);
        }
        return new NoOpJobManagementReconciler();
    }
}
