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

package com.netflix.titus.master.kubernetes.pod;

import java.util.Arrays;
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.archaius.api.Config;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.master.kubernetes.pod.env.ContainerEnvFactory;
import com.netflix.titus.master.kubernetes.pod.env.DefaultAggregatingContainerEnvFactory;
import com.netflix.titus.master.kubernetes.pod.env.TitusProvidedContainerEnvFactory;
import com.netflix.titus.master.kubernetes.pod.env.UserProvidedContainerEnvFactory;
import com.netflix.titus.master.kubernetes.pod.resourcepool.CapacityGroupPodResourcePoolResolver;
import com.netflix.titus.master.kubernetes.pod.resourcepool.ExplicitJobPodResourcePoolResolver;
import com.netflix.titus.master.kubernetes.pod.resourcepool.FarzonePodResourcePoolResolver;
import com.netflix.titus.master.kubernetes.pod.resourcepool.GpuPodResourcePoolResolver;
import com.netflix.titus.master.kubernetes.pod.resourcepool.PodResourcePoolResolver;
import com.netflix.titus.master.kubernetes.pod.resourcepool.PodResourcePoolResolverChain;
import com.netflix.titus.master.kubernetes.pod.resourcepool.PodResourcePoolResolverFeatureGuard;
import com.netflix.titus.master.kubernetes.pod.resourcepool.TierPodResourcePoolResolver;
import com.netflix.titus.master.kubernetes.pod.v0.V0SpecPodFactory;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;

public class KubePodModule extends AbstractModule {

    private static final String RESOURCE_POOL_PROPERTIES_PREFIX = "titus.resourcePools";

    @Override
    protected void configure() {
        bind(PodFactory.class).to(V0SpecPodFactory.class);
    }

    @Provides
    @Singleton
    public KubePodConfiguration getKubePodConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(KubePodConfiguration.class);
    }

    @Provides
    @Singleton
    public PodResourcePoolResolver getPodResourcePoolResolver(KubePodConfiguration configuration,
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
    public ContainerEnvFactory getContainerEnvFactory(TitusRuntime titusRuntime) {
        return new DefaultAggregatingContainerEnvFactory(titusRuntime,
                UserProvidedContainerEnvFactory.getInstance(),
                TitusProvidedContainerEnvFactory.getInstance());
    }
}
