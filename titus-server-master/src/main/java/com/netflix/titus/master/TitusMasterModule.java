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

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.governator.guice.jersey.GovernatorJerseySupportModule;
import com.netflix.titus.master.agent.AgentModule;
import com.netflix.titus.master.agent.endpoint.AgentEndpointModule;
import com.netflix.titus.master.appscale.endpoint.v3.AutoScalingModule;
import com.netflix.titus.master.audit.service.AuditModule;
import com.netflix.titus.master.cluster.DefaultLeaderActivator;
import com.netflix.titus.master.cluster.DefaultLeaderElector;
import com.netflix.titus.master.cluster.LeaderActivator;
import com.netflix.titus.master.cluster.LeaderElector;
import com.netflix.titus.master.config.CellInfoResolver;
import com.netflix.titus.master.config.ConfigurableCellInfoResolver;
import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.endpoint.EndpointModule;
import com.netflix.titus.master.endpoint.common.ContextResolver;
import com.netflix.titus.master.endpoint.common.EmptyContextResolver;
import com.netflix.titus.master.endpoint.v2.rest.JerseyModule;
import com.netflix.titus.master.endpoint.v2.validator.ValidatorConfiguration;
import com.netflix.titus.master.eviction.endpoint.grpc.EvictionEndpointModule;
import com.netflix.titus.master.eviction.service.EvictionServiceModule;
import com.netflix.titus.master.health.HealthModule;
import com.netflix.titus.master.job.JobModule;
import com.netflix.titus.master.jobmanager.endpoint.v3.V3EndpointModule;
import com.netflix.titus.master.jobmanager.service.V3JobManagerModule;
import com.netflix.titus.master.loadbalancer.LoadBalancerModule;
import com.netflix.titus.master.master.MasterDescription;
import com.netflix.titus.master.master.MasterMonitor;
import com.netflix.titus.master.master.ZookeeperMasterMonitor;
import com.netflix.titus.master.mesos.MesosModule;
import com.netflix.titus.master.scheduler.SchedulerModule;
import com.netflix.titus.master.service.management.ManagementModule;
import com.netflix.titus.master.store.StoreModule;
import com.netflix.titus.master.taskmigration.TaskMigratorModule;
import com.netflix.titus.master.zookeeper.ZookeeperPaths;
import com.netflix.titus.runtime.TitusEntitySanitizerModule;
import com.netflix.titus.runtime.endpoint.common.EmptyLogStorageInfo;
import com.netflix.titus.runtime.endpoint.resolver.ByRemoteAddressHttpCallerIdResolver;
import com.netflix.titus.runtime.endpoint.resolver.HostCallerIdResolver;
import com.netflix.titus.runtime.endpoint.resolver.HttpCallerIdResolver;
import com.netflix.titus.runtime.endpoint.resolver.NoOpHostCallerIdResolver;

/**
 * Main TitusMaster guice module.
 */
public class TitusMasterModule extends AbstractModule {

    private final boolean enableREST;

    public TitusMasterModule() {
        this(true);
    }

    public TitusMasterModule(boolean enableREST) {
        this.enableREST = enableREST;
    }

    @Override
    protected void configure() {
        // Configuration
        bind(CoreConfiguration.class).to(MasterConfiguration.class);
        bind(CellInfoResolver.class).to(ConfigurableCellInfoResolver.class);

        install(new TitusEntitySanitizerModule());

        // Mesos
        install(new MesosModule());

        // TitusMaster monitor / leader election
        bind(MasterMonitor.class).to(ZookeeperMasterMonitor.class);
        bind(LeaderElector.class).to(DefaultLeaderElector.class).asEagerSingleton();
        bind(LeaderActivator.class).to(DefaultLeaderActivator.class);

        // Storage
        install(new StoreModule());

        // Service
        install(new AuditModule());
        install(new AgentModule());
        install(new SchedulerModule());
        install(new JobModule());
        install(new V3JobManagerModule());
        install(new ManagementModule());

        // Remaining services
        bind(ApiOperations.class).to(ApiOperationsImpl.class);

        // REST/GRPC
        bind(JerseyModule.V2_LOG_STORAGE_INFO).toInstance(EmptyLogStorageInfo.INSTANCE);
        bind(V3EndpointModule.V3_LOG_STORAGE_INFO).toInstance(EmptyLogStorageInfo.INSTANCE);
        bind(ContextResolver.class).toInstance(EmptyContextResolver.INSTANCE);

        if (enableREST) {
            install(new GovernatorJerseySupportModule());

            // This should be in JerseyModule, but overrides get broken if we do that (possibly Governator bug).
            bind(HttpCallerIdResolver.class).to(ByRemoteAddressHttpCallerIdResolver.class);
            bind(HostCallerIdResolver.class).to(NoOpHostCallerIdResolver.class);
            install(new JerseyModule());
        }

        install(new EndpointModule());
        install(new HealthModule());
        install(new V3EndpointModule());
        install(new AgentEndpointModule());
        install(new AutoScalingModule());
        install(new LoadBalancerModule());

        install(new EvictionServiceModule());
        install(new EvictionEndpointModule());

        install(new TaskMigratorModule());
    }

    @Provides
    @Singleton
    public MasterConfiguration getMasterConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(MasterConfiguration.class);
    }

    @Provides
    @Singleton
    public ValidatorConfiguration getValidatorConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(ValidatorConfiguration.class);
    }

    @Provides
    @Singleton
    public MasterDescription getMasterDescription(MasterConfiguration configuration) {
        return MasterDescriptions.create(configuration);
    }

    @Provides
    @Singleton
    public ZookeeperPaths getZookeeperPaths(MasterConfiguration configuration) {
        return new ZookeeperPaths(configuration.getZkRoot());
    }
}
