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

package com.netflix.titus.master;

import javax.inject.Singleton;

import com.google.common.base.Preconditions;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.multibindings.Multibinder;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.governator.guice.jersey.GovernatorJerseySupportModule;
import com.netflix.titus.api.containerhealth.service.ContainerHealthService;
import com.netflix.titus.api.supervisor.service.MasterDescription;
import com.netflix.titus.master.appscale.endpoint.v3.AutoScalingModule;
import com.netflix.titus.master.audit.service.AuditModule;
import com.netflix.titus.master.config.CellInfoResolver;
import com.netflix.titus.master.config.ConfigurableCellInfoResolver;
import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.endpoint.MasterEndpointModule;
import com.netflix.titus.master.endpoint.admission.JobCoordinatorAdmissionModule;
import com.netflix.titus.master.endpoint.common.ContextResolver;
import com.netflix.titus.master.endpoint.common.EmptyContextResolver;
import com.netflix.titus.master.endpoint.v2.rest.JerseyModule;
import com.netflix.titus.master.eviction.endpoint.grpc.EvictionEndpointModule;
import com.netflix.titus.master.eviction.service.EvictionServiceModule;
import com.netflix.titus.master.health.HealthModule;
import com.netflix.titus.master.jobmanager.endpoint.v3.V3EndpointModule;
import com.netflix.titus.master.jobmanager.service.V3JobManagerModule;
import com.netflix.titus.master.kubernetes.KubeModule;
import com.netflix.titus.master.kubernetes.controller.KubeControllerModule;
import com.netflix.titus.master.kubernetes.pod.KubePodModule;
import com.netflix.titus.master.loadbalancer.LoadBalancerModule;
import com.netflix.titus.master.scheduler.SchedulerModule;
import com.netflix.titus.master.service.management.ManagementModule;
import com.netflix.titus.master.store.StoreModule;
import com.netflix.titus.master.supervisor.endpoint.SupervisorEndpointModule;
import com.netflix.titus.master.supervisor.service.SupervisorServiceModule;
import com.netflix.titus.runtime.FeatureFlagModule;
import com.netflix.titus.runtime.TitusEntitySanitizerModule;
import com.netflix.titus.runtime.containerhealth.service.AlwaysHealthyContainerHealthService;
import com.netflix.titus.runtime.containerhealth.service.ContainerHealthServiceModule;
import com.netflix.titus.runtime.endpoint.common.EmptyLogStorageInfo;
import com.netflix.titus.runtime.endpoint.resolver.ByRemoteAddressHttpCallerIdResolver;
import com.netflix.titus.runtime.endpoint.resolver.HostCallerIdResolver;
import com.netflix.titus.runtime.endpoint.resolver.HttpCallerIdResolver;
import com.netflix.titus.runtime.endpoint.resolver.NoOpHostCallerIdResolver;

/**
 * Main TitusMaster guice module.
 */
public class TitusMasterModule extends AbstractModule {

    public enum Mode {
        KUBE,
        EMBEDDED_KUBE,
    }

    private final boolean enableREST;
    private final Mode mode;

    public TitusMasterModule() {
        this(true, Mode.KUBE);
    }

    public TitusMasterModule(boolean enableREST, Mode mode) {
        this.enableREST = enableREST;
        this.mode = mode;
    }

    @Override
    protected void configure() {
        // Configuration
        bind(CellInfoResolver.class).to(ConfigurableCellInfoResolver.class);

        // Titus supervisor
        install(new SupervisorServiceModule());
        install(new SupervisorEndpointModule());

        install(new TitusEntitySanitizerModule());

        // Feature flags
        install(new FeatureFlagModule());

        // Kubernetes
        Preconditions.checkNotNull(mode, "Kube mode not set");
        if (mode == Mode.KUBE) {
            install(new KubeModule());
        } else if (mode == Mode.EMBEDDED_KUBE) {
            install(new KubeControllerModule());
            install(new KubePodModule());
        }

        // Storage
        install(new StoreModule());

        // Service
        install(new AuditModule());
        install(new SchedulerModule());
        install(new V3JobManagerModule());

        install(new ContainerHealthServiceModule());
        Multibinder.newSetBinder(binder(), ContainerHealthService.class).addBinding().to(AlwaysHealthyContainerHealthService.class);

        install(new ManagementModule());

        // REST/GRPC
        bind(V3EndpointModule.V3_LOG_STORAGE_INFO).toInstance(EmptyLogStorageInfo.INSTANCE);
        bind(ContextResolver.class).toInstance(EmptyContextResolver.INSTANCE);

        if (enableREST) {
            install(new GovernatorJerseySupportModule());

            // This should be in JerseyModule, but overrides get broken if we do that (possibly Governator bug).
            bind(HttpCallerIdResolver.class).to(ByRemoteAddressHttpCallerIdResolver.class);
            bind(HostCallerIdResolver.class).to(NoOpHostCallerIdResolver.class);
            install(new JerseyModule());
        }

        install(new JobCoordinatorAdmissionModule());
        install(new MasterEndpointModule());
        install(new HealthModule());
        install(new V3EndpointModule());
        install(new AutoScalingModule());
        install(new LoadBalancerModule());

        install(new EvictionServiceModule());
        install(new EvictionEndpointModule());
    }

    @Provides
    @Singleton
    public MasterConfiguration getMasterConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(MasterConfiguration.class);
    }

    @Provides
    @Singleton
    public MasterDescription getMasterDescription(MasterConfiguration configuration) {
        return MasterDescriptions.create(configuration);
    }
}
